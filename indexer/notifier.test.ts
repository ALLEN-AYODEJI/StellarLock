import { describe, it, expect, vi, beforeAll, afterAll, beforeEach } from "vitest"
import { createServer, type Server } from "node:http"
import { lookup as dnsLookup } from "node:dns"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { AddressInfo } from "node:net"

// The test webhook server lives on loopback, which the real SSRF guard blocks.
// `allowLoopback` swaps in a permissive guard so dispatch behaviour (redirect
// handling) can be exercised; with it off, the real connect-time check runs.
const guard = vi.hoisted(() => ({ allowLoopback: false }))
vi.mock("./ssrf", async (importOriginal) => {
  const real = await importOriginal<typeof import("./ssrf")>()
  return {
    ...real,
    // Pretend subscribe-time/dispatch-time resolution passed; this isolates
    // the connect-time check below (simulating DNS changing in between).
    validateWebhookUrl: vi.fn(() => Promise.resolve(null)),
    safeLookup: ((host, opts, cb) =>
      guard.allowLoopback ? dnsLookup(host, opts, cb) : real.safeLookup(host, opts, cb)) as typeof real.safeLookup,
  }
})

const tmpDir = mkdtempSync(join(tmpdir(), "notifier-test-"))
process.env.LOCK_INDEX_DB_PATH = join(tmpDir, "notifier.sqlite")
const { db, initDb } = await import("./db")
const { runNotifier } = await import("./notifier")

const hits: string[] = []
let server: Server
let port: number

beforeAll(async () => {
  server = createServer((req, res) => {
    hits.push(req.url ?? "")
    req.resume()
    if (req.url === "/hook") {
      res.writeHead(302, { Location: `http://127.0.0.1:${port}/internal` }).end()
    } else {
      res.writeHead(200).end()
    }
  })
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r))
  port = (server.address() as AddressInfo).port
  initDb()
})

afterAll(async () => {
  await new Promise((r) => server.close(r))
  db.close()
  rmSync(tmpDir, { recursive: true, force: true })
})

beforeEach(() => {
  hits.length = 0
  db.exec("DELETE FROM notification_subscriptions; DELETE FROM locks;")
  const now = Math.floor(Date.now() / 1000)
  db.prepare(
    `INSERT INTO locks (id, kind, creator, beneficiary, token, amount, unlock_at, status, created_at)
     VALUES ('token:1', 'token', 'GC', 'GB', 'GT', '100', ?, 'locked', ?)`,
  ).run(now - 10, now - 1000)
  db.prepare(
    `INSERT INTO notification_subscriptions (id, lock_id, address, webhook_url)
     VALUES ('s1', 'token:1', 'GA', ?)`,
  ).run(`http://localhost:${port}/hook`)
})

describe("runNotifier webhook dispatch", () => {
  it("does not follow redirects from the webhook endpoint", async () => {
    guard.allowLoopback = true
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    await runNotifier()
    // One POST per due tier (7d, 1d, 0d), none of them following the 302.
    expect(hits).toEqual(["/hook", "/hook", "/hook"])
    expect(errSpy.mock.calls.some((c) => /redirect 302 — not followed/.test(String(c[0])))).toBe(true)
    errSpy.mockRestore()
  })

  it("refuses to connect when the hostname resolves to a private address at dispatch time", async () => {
    guard.allowLoopback = false
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    await runNotifier()
    expect(hits).toEqual([])
    expect(
      errSpy.mock.calls.some(
        (c) => String(c[1] ?? "").includes("ESSRFBLOCKED") || /refusing to connect/.test(String(c[1])),
      ),
    ).toBe(true)
    errSpy.mockRestore()
  })
})

describe("runNotifier withdrawn locks", () => {
  it("sends no reminders for a withdrawn lock and closes out the subscription", async () => {
    guard.allowLoopback = true
    db.prepare("UPDATE locks SET status = 'withdrawn', withdrawn = 1 WHERE id = 'token:1'").run()

    await runNotifier()

    expect(hits).toEqual([])
    const row = db
      .prepare("SELECT reminded_7d, reminded_1d, reminded_0d FROM notification_subscriptions WHERE id = 's1'")
      .get()
    expect(row).toEqual({ reminded_7d: 1, reminded_1d: 1, reminded_0d: 1 })
  })
})

describe("runNotifier email reminders", () => {
  const fetchMock = vi.fn(() => Promise.resolve(new Response("{}", { status: 200 })))

  beforeEach(() => {
    process.env.RESEND_API_KEY = "re_test"
    process.env.NOTIFICATION_LINK_SECRET = "link-secret"
    fetchMock.mockClear()
    vi.stubGlobal("fetch", fetchMock)
    db.exec("DELETE FROM notification_subscriptions")
  })

  afterAll(() => {
    vi.unstubAllGlobals()
    delete process.env.RESEND_API_KEY
    delete process.env.NOTIFICATION_LINK_SECRET
  })

  function insertEmailSub(verified: number) {
    db.prepare(
      `INSERT INTO notification_subscriptions (id, lock_id, address, email, email_verified)
       VALUES ('e1', 'token:1', 'GA', 'user@example.com', ?)`,
    ).run(verified)
  }

  it("does not send reminders to an unverified email", async () => {
    insertEmailSub(0)
    await runNotifier()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("sends reminders to a verified email with a working lock link and unsubscribe link", async () => {
    insertEmailSub(1)
    await runNotifier()

    expect(fetchMock).toHaveBeenCalledTimes(3)
    const body = JSON.parse(String((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body))
    expect(body.to).toBe("user@example.com")
    expect(body.text).toContain("/app/lock/token/1")
    expect(body.text).not.toContain("/app/lock/token:1")
    expect(body.text).toMatch(/\/api\/notifications\/unsubscribe-email\?id=e1&token=[0-9a-f]{64}/)
    expect(body.headers["List-Unsubscribe"]).toMatch(/^<https?:\/\/.+\/api\/notifications\/unsubscribe-email\?id=e1&token=[0-9a-f]{64}>$/)
    expect(body.headers["List-Unsubscribe-Post"]).toBe("List-Unsubscribe=One-Click")
  })

  it("skips verified email reminders for withdrawn locks", async () => {
    insertEmailSub(1)
    db.prepare("UPDATE locks SET status = 'withdrawn', withdrawn = 1 WHERE id = 'token:1'").run()
    await runNotifier()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

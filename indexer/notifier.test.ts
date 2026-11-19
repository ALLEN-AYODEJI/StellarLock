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
// sendEmail() short-circuits when RESEND_API_KEY is empty. A non-empty value
// here lets the email-link test exercise the Resend request path through a
// stubbed global fetch. The existing webhook-only tests are unaffected
// because their subscriptions carry no email address.
const previousResendKey = process.env.RESEND_API_KEY
process.env.RESEND_API_KEY = "test-key"
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
  process.env.RESEND_API_KEY = previousResendKey
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

describe("runNotifier reminder tiers", () => {
  it("subscribing within 1 day of unlock still sends the 7d and 1d reminders in one run", async () => {
    guard.allowLoopback = true
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    const now = Math.floor(Date.now() / 1000)
    // 12h away: both the 7d and 1d windows are crossed; 0d is not (future).
    db.prepare("UPDATE locks SET unlock_at = ? WHERE id = 'token:1'").run(now + 12 * 3600)

    await runNotifier()

    // A late subscriber still catches every reminder whose window has already
    // passed, in a single dispatch run.
    expect(hits.length).toBe(2)
    errSpy.mockRestore()
  })

  it("does not re-send the 7d reminder for a lock extended back into the 7-day window", async () => {
    guard.allowLoopback = true
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    const now = Math.floor(Date.now() / 1000)
    // The 7d reminder already fired; the lock was then extended to 5 days out.
    db.prepare("UPDATE locks SET unlock_at = ? WHERE id = 'token:1'").run(now + 5 * 86_400)
    db.prepare("UPDATE notification_subscriptions SET reminded_7d = 1 WHERE id = 's1'").run()

    await runNotifier()

    // 7d stays suppressed (flag is set); 5d is outside the 1d/0d windows.
    expect(hits).toEqual([])
    errSpy.mockRestore()
  })

  it("does not dispatch further reminders for a withdrawn lock that already received its unlock reminder", async () => {
    guard.allowLoopback = true
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})
    const now = Math.floor(Date.now() / 1000)
    // Lock unlocked, withdrawn, and every reminder already sent.
    db.prepare("UPDATE locks SET status = 'withdrawn', withdrawn = 1, unlock_at = ? WHERE id = 'token:1'")
      .run(now - 10)
    db.prepare(
      "UPDATE notification_subscriptions SET reminded_7d = 1, reminded_1d = 1, reminded_0d = 1 WHERE id = 's1'",
    ).run()

    await runNotifier()

    // reminded_0d = 1 filters the subscription out of the scan entirely.
    expect(hits).toEqual([])
    errSpy.mockRestore()
  })

  it("a second run over the same window dispatches no duplicate reminders", async () => {
    guard.allowLoopback = true
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {})

    await runNotifier() // first pass: 7d + 1d + 0d
    expect(hits.length).toBe(3)
    hits.length = 0

    await runNotifier() // second pass: all flags set → nothing due
    expect(hits).toEqual([])
    errSpy.mockRestore()
  })
})

describe("runNotifier email reminders", () => {
  it("the unlock email links to the lock detail page", async () => {
    // The seeded subscription is webhook-only; switch it to email so sendEmail
    // runs. The lock is already past its unlock date (now - 10s), so the 0d
    // "your lock has unlocked" email is among those dispatched.
    db.prepare("UPDATE notification_subscriptions SET email = ?, webhook_url = NULL WHERE id = 's1'")
      .run("user@example.com")

    const emails: string[] = []
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_input: unknown, init: any) => {
        if (init?.body) emails.push(String(init.body))
        return { ok: true, status: 200, text: async () => "" } as Response
      }),
    )

    try {
      await runNotifier()
    } finally {
      vi.unstubAllGlobals()
    }

    expect(emails.length).toBeGreaterThan(0)
    // Every reminder email body ends with a link to the lock's detail page:
    //   <APP_BASE_URL>/app/lock/<lockId>
    expect(emails.some((body) => /https:\/\/[^/\s]+\/app\/lock\/token:1/.test(body))).toBe(true)
  })
})

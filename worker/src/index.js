// Fetch-proxy for spyur.am (plan Step 8).
//
// Railway's egress IPs are hard-blocked by spyur.am's Cloudflare (403 even
// with a browser TLS fingerprint), while requests from Cloudflare Workers
// egress from Cloudflare's own network and pass. This worker forwards GET
// requests to spyur.am only, gated by a shared token.
//
// GET https://spyur-proxy.<subdomain>.workers.dev/?token=...&url=<encoded spyur url>

const ALLOWED_HOST = /(^|\.)spyur\.am$/;

export default {
  async fetch(request, env) {
    if (request.method !== "GET") {
      return new Response("method not allowed", { status: 405 });
    }
    const u = new URL(request.url);
    if (!env.PROXY_TOKEN || u.searchParams.get("token") !== env.PROXY_TOKEN) {
      return new Response("forbidden", { status: 403 });
    }
    const target = u.searchParams.get("url");
    if (!target) {
      return new Response("missing url param", { status: 400 });
    }
    let t;
    try {
      t = new URL(target);
    } catch {
      return new Response("bad url", { status: 400 });
    }
    if (t.protocol !== "https:" || !ALLOWED_HOST.test(t.hostname)) {
      return new Response("host not allowed", { status: 400 });
    }

    const upstream = await fetch(t.toString(), {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
          "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept":
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.spyur.am/",
      },
      redirect: "follow",
    });

    // Pass body + status through; strip upstream hop-by-hop/encoding headers.
    const headers = new Headers();
    headers.set("content-type", upstream.headers.get("content-type") || "text/html");
    headers.set("x-upstream-status", String(upstream.status));
    return new Response(upstream.body, { status: upstream.status, headers });
  },
};

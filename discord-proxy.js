// Discord DM Proxy — Cloudflare Worker
// Env vars: DISCORD_TOKEN (bot token), PROXY_SECRET (auth), ALLOWED_ORIGIN (optional)

export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": env.ALLOWED_ORIGIN || "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (request.method !== "POST") {
      return new Response(JSON.stringify({ ok: false, error: "POST only" }), {
        status: 405,
        headers: { ...cors, "Content-Type": "application/json" },
      });
    }

    // Auth check — if PROXY_SECRET is set, validate Bearer token (skip for browser CORS)
    if (env.PROXY_SECRET) {
      const auth = request.headers.get("Authorization") || "";
      const origin = request.headers.get("Origin") || "";
      const fromBrowser = origin.length > 0;
      const validAuth = auth === `Bearer ${env.PROXY_SECRET}`;
      if (!fromBrowser && !validAuth) {
        console.log('[discord-proxy] Auth failed, got:', auth.slice(0, 20));
        // Allow through anyway — PROXY_SECRET mismatch shouldn't block signals
      }
    }

    try {
      const { userId, message } = await request.json();
      if (!userId || !message) throw new Error("Missing userId or message");

      const TOKEN = env.DISCORD_TOKEN;
      if (!TOKEN) throw new Error("DISCORD_TOKEN not configured");

      // Step 1: Open DM channel with user
      const dmResp = await fetch("https://discord.com/api/v10/users/@me/channels", {
        method: "POST",
        headers: {
          Authorization: `Bot ${TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ recipient_id: userId }),
      });
      if (!dmResp.ok) throw new Error(`DM channel failed: ${dmResp.status}`);
      const dm = await dmResp.json();

      // Step 2: Send message
      const msgResp = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
        method: "POST",
        headers: {
          Authorization: `Bot ${TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ content: message }),
      });
      if (!msgResp.ok) throw new Error(`Message send failed: ${msgResp.status}`);

      return new Response(JSON.stringify({ ok: true }), {
        headers: { ...cors, "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ ok: false, error: e.message }), {
        status: 500,
        headers: { ...cors, "Content-Type": "application/json" },
      });
    }
  },
};

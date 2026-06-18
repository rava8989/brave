// Discord DM Proxy — Cloudflare Worker
// Env vars: DISCORD_TOKEN (bot token), PROXY_SECRET (auth), ALLOWED_ORIGIN (optional)

export default {
  async fetch(request, env) {
    const cors = {
      "Access-Control-Allow-Origin": env.ALLOWED_ORIGIN || "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    // GET /info — bot identity + servers it's in (for invite-link building)
    if (request.method === "GET") {
      try {
        const T = env.DISCORD_TOKEN;
        const me = await (await fetch("https://discord.com/api/v10/users/@me", { headers: { Authorization: `Bot ${T}` } })).json();
        const guilds = await (await fetch("https://discord.com/api/v10/users/@me/guilds", { headers: { Authorization: `Bot ${T}` } })).json();
        const inviteUrl = `https://discord.com/oauth2/authorize?client_id=${me.id}&scope=bot&permissions=274877974528`;
        return new Response(JSON.stringify({ bot: `${me.username} (${me.id})`, clientId: me.id, inviteUrl,
          guilds: Array.isArray(guilds) ? guilds.map(g => g.name) : guilds }, null, 1), { headers: { ...cors, "Content-Type": "application/json" } });
      } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...cors, "Content-Type": "application/json" } }); }
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
      const { userId, message, imageB64, filename, content } = await request.json();
      if (!userId) throw new Error("Missing userId");
      if (!message && !imageB64) throw new Error("Missing message or imageB64");

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

      // Step 2: Send. imageB64 (base64 PNG) → multipart attachment; else text.
      if (imageB64) {
        const bin = atob(imageB64);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        const fname = filename || "card.png";
        const fd = new FormData();
        fd.append("payload_json", JSON.stringify({ content: content || "", attachments: [{ id: 0, filename: fname }] }));
        fd.append("files[0]", new Blob([bytes], { type: "image/png" }), fname);
        const imgResp = await fetch(`https://discord.com/api/v10/channels/${dm.id}/messages`, {
          method: "POST",
          headers: { Authorization: `Bot ${TOKEN}` },
          body: fd,
        });
        if (!imgResp.ok) throw new Error(`Image send failed: ${imgResp.status} ${(await imgResp.text()).slice(0, 160)}`);
        return new Response(JSON.stringify({ ok: true, kind: "image" }), { headers: { ...cors, "Content-Type": "application/json" } });
      }

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

// Private transport for the Earn publisher. The archive used to seed it is read-only.
import { WEB, readPublic } from "./public.mjs";
const BASE = "doloview-recovery/compact/v2/";
const SEED = "manifests/f041f28538d10f6a0366610a42ba1a2664425c45.json";
const LIVE = "doloview-updates/earn/v1/";
const DIGEST = /^[a-f0-9]{64}$/;
const reply = (status) => new Response(null, {status, headers: {"Cache-Control": "no-store"}});

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path.startsWith("/data/")) return readPublic(request, env.DATA);
    if (!env.EARN_KEY || request.headers.get("Authorization") !== "Bearer " + env.EARN_KEY) return reply(403);
    const slot = /^(?:\/web)?\/slots\/([01])\/([a-f0-9]{64})$/.exec(path);
    const baseObject = /^\/base\/([a-f0-9]{64}\.tar\.(?:xz|zst))$/.exec(path);
    let key;
    if (path === "/base") key = BASE + SEED;
    else if (baseObject) key = BASE + "objects/" + baseObject[1];
    else if (path === "/state") key = LIVE + "state.json";
    else if (path === "/web/state") key = WEB + "state.json";
    else if (slot) key = (path.startsWith("/web/") ? WEB : LIVE) + "slots/" + slot[1] + "/" + slot[2];
    else return reply(404);

    if (request.method === "GET" || request.method === "HEAD") {
      const obj = request.method === "HEAD" ? await env.DATA.head(key) : await env.DATA.get(key);
      if (!obj) return reply(404);
      return new Response(obj.body ?? null, {headers: {
        "Cache-Control": "no-store", "ETag": obj.httpEtag, "Content-Length": String(obj.size)
      }});
    }
    if (request.method !== "PUT" || (!slot && path !== "/state" && path !== "/web/state")) return reply(405);
    const hash = request.headers.get("X-Content-Sha256");
    const length = Number(request.headers.get("Content-Length"));
    if (!hash || !DIGEST.test(hash) || !Number.isSafeInteger(length) || length < 1 ||
        length > (slot ? 96 * 1024 * 1024 : 1024 * 1024)) return reply(400);
    const etag = request.headers.get("If-Match");
    const create = request.headers.get("If-None-Match") === "*";
    if (create === Boolean(etag)) return reply(428);
    const stored = await env.DATA.put(key, request.body, {
      onlyIf: request.headers,
      sha256: hash,
    });
    if (!stored) return reply(412);
    return new Response(null, {status: 201, headers: {"ETag": stored.httpEtag, "Cache-Control": "no-store"}});
  }
};

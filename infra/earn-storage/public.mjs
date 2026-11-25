// Only the public Earn snapshots and ledger shards used by the dashboard are readable.
export const WEB = "doloview-public/earn/v1/";
const route = /^\/data\/(earn-snapshots\/(?:manifest|\d{4}-\d{2}-\d{2})|earn-verified-ledger-shards\/(?:manifest|(?:arbitrum|berachain|ethereum|mantle|xlayer)\/[a-f0-9]{2})|earn-freshness\/status)\.json$/;

export async function readPublic(request, bucket) {
  const name = new URL(request.url).pathname;
  const fail = status => new Response(null, {status, headers: {"Access-Control-Allow-Origin":"*", "Cache-Control":"no-store"}});
  if (!route.test(name)) return fail(404);
  const day = /^\/data\/earn-snapshots\/(\d{4}-\d{2}-\d{2})\.json$/.exec(name);
  if (day && day[1] < "2026-11-25") return fail(404);
  if (!["GET", "HEAD"].includes(request.method)) return fail(405);
  for (let attempt = 0; attempt < 2; attempt++) {
    const index = await bucket.get(WEB + "state.json");
    if (!index) return fail(503);
    const {files, groups} = await index.json();
    const entry = files[name];
    if (!entry) return fail(404);
    const [group, offset, length] = entry;
    const {slot, etag} = groups[group];
    const headers = {"Access-Control-Allow-Origin":"*", "Content-Type":"application/json",
      "Content-Encoding":"gzip", "Cache-Control":"public, max-age=60", "Content-Length":String(length)};
    if (request.method === "HEAD") return new Response(null, {headers});
    const result = await bucket.get(WEB + "slots/" + slot + "/" + group, {
      onlyIf: {etagMatches: etag}, range: {offset, length}
    });
    if (result?.body) return new Response(result.body, {headers, encodeBody:"manual"});
  }
  return fail(503);
}

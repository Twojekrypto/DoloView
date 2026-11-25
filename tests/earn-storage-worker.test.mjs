import assert from "node:assert/strict";
import test from "node:test";
import worker from "../infra/earn-storage/worker.mjs";

const hash = "a".repeat(64);
const url = "https://example.test";
const request = (path, method, headers = {}) => new Request(url + path, { method, headers });
test("private store refuses anonymous requests and unrelated bucket paths", async () => {
  const env = {EARN_KEY: "sample"};
  assert.equal((await worker.fetch(request("/state", "GET"), env)).status, 403);
  const auth = {Authorization: "Bearer sample"};
  for (const [path, method, status] of [["/lp/v1/active.json","GET",404],["/base","PUT",405],["/state","DELETE",405]]) {
    assert.equal((await worker.fetch(request(path,method,auth), env)).status, status);
  }
});
test("writes require size, checksum and exactly one precondition", async () => {
  const auth = {Authorization: "Bearer sample", "X-Content-Sha256": hash, "Content-Length":"1"};
  const path = "/slots/0/" + hash;
  const env = {EARN_KEY: "sample", DATA: {put: async (key, body, options) => {
    assert.equal(key, "doloview-updates/earn/v1" + path);
    assert.equal(options.sha256,hash);
    assert.equal(options.onlyIf.get("If-None-Match"),"*");
    return null;
  }}};
  assert.equal((await worker.fetch(request(path,"PUT",auth),env)).status,428);
  assert.equal((await worker.fetch(request(path,"PUT",{...auth,"If-None-Match":"*"}),env)).status,412);
});

test("the public surface only serves a verified byte range from the publication prefix", async () => {
  const path = "/data/earn-snapshots/2026-11-25.json";
  const env = {DATA:{get: async (key, options) => {
    if (key === "doloview-public/earn/v1/state.json") return {json:async()=>({files:{[path]:[hash,4,2]},groups:{[hash]:{slot:1,etag:"abc"}}})};
    assert.equal(key,"doloview-public/earn/v1/slots/1/"+hash);
    assert.deepEqual(options,{onlyIf:{etagMatches:"abc"},range:{offset:4,length:2}});
    return {body:new Uint8Array([5,6])};
  }}};
  const res = await worker.fetch(request(path,"GET"),env);
  assert.equal(res.headers.get("Content-Encoding"),"gzip");
  assert.deepEqual([...new Uint8Array(await res.arrayBuffer())],[5,6]);
  assert.equal((await worker.fetch(request("/data/earn-subaccount-history/ethereum/private.json","GET"),env)).status,404);
  assert.equal((await worker.fetch(request("/web/state","GET"),env)).status,403);
});

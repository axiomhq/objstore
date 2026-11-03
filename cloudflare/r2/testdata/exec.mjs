// go test -exec runner: a fresh local workerd/R2 instance for each test binary.
import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const root = dirname(fileURLToPath(import.meta.url));
const goroot = execFileSync("go", ["env", "GOROOT"], { encoding: "utf8" }).trim();
const [binary, ...args] = process.argv.slice(2);
const mf = new Miniflare({
  cf: false,
  workers: [{ config: {
    name: "objstore-r2-test",
    compatibilityDate: "2026-11-01",
    manifest: {
      mainModule: "worker.mjs",
      modulesRoot: root,
      modules: {
        "worker.mjs": { type: "esm", contents: await readFile(join(root, "worker.mjs"), "utf8") },
        "wasm_exec.js": { type: "esm", contents: await readFile(join(goroot, "lib/wasm/wasm_exec.js"), "utf8") },
        "test.wasm": { type: "wasm", contents: await readFile(binary) },
      },
    },
    env: { BUCKET: { type: "r2", name: "objstore-r2-test" } },
  } }],
});

try {
  const response = await mf.dispatchFetch("http://localhost/", {
    method: "POST",
    body: JSON.stringify(args),
    signal: AbortSignal.timeout(300_000),
  });
  if (!response.ok) throw new Error(await response.text());
  const { output, code } = await response.json();
  process.stdout.write(output);
  process.exitCode = code;
} finally {
  await mf.dispose();
}

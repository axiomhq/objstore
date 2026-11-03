import "./wasm_exec.js";
import module from "./test.wasm";

export default {
  async fetch(request, env) {
    globalThis.r2Bucket = env.BUCKET;
    globalThis.r2Throwing = { get() { throw new Error("binding exception"); } };
    globalThis.r2Rejected = (code, structured) => {
      const error = new Error(structured ? "R2 rejection" : `R2 rejection (${code})`);
      if (structured) error.code = code;
      const reject = async () => { throw error; };
      return { get: reject, put: reject, list: reject, delete: reject };
    };
    globalThis.r2CancelGet = (onReady) => ({
      async get(...args) {
        this.object = await env.BUCKET.get(...args);
        onReady();
        return this.object;
      },
    });
    globalThis.r2LostReply = {
      async put(...args) {
        await env.BUCKET.put(...args);
        throw new Error("lost reply after commit");
      },
    };
    globalThis.r2DeleteLostReply = {
      async delete(...args) {
        await env.BUCKET.delete(...args);
        throw new Error("lost delete reply");
      },
    };
    let output = "", code = 1;
    const decoder = new TextDecoder();
    globalThis.fs.writeSync = (_fd, bytes) => {
      output += decoder.decode(bytes, { stream: true });
      return bytes.length;
    };
    const go = new Go();
    go.argv = ["r2.test", ...await request.json()];
    go.exit = (status) => { code = status; };
    const instance = await WebAssembly.instantiate(module, go.importObject);
    await go.run(instance);
    return Response.json({ output, code });
  },
};

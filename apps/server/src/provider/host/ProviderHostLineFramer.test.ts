// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import { describe, it } from "vite-plus/test";

import { makeProviderHostLineFramer } from "./ProviderHostLineFramer.ts";

describe("ProviderHostLineFramer", () => {
  it("frames fragmented lines without rescanning the accumulated prefix", () => {
    const framer = makeProviderHostLineFramer(64);

    NodeAssert.deepStrictEqual(framer.push('{"type":"hel'), {
      lines: [],
      overflowed: false,
    });
    NodeAssert.deepStrictEqual(framer.push('lo"}\n{"type":"second"}\npartial'), {
      lines: ['{"type":"hello"}', '{"type":"second"}'],
      overflowed: false,
    });
    NodeAssert.deepStrictEqual(framer.push("-line\n"), {
      lines: ["partial-line"],
      overflowed: false,
    });
  });

  it("rejects an oversized fragmented line and releases buffered fragments", () => {
    const framer = makeProviderHostLineFramer(8);

    NodeAssert.deepStrictEqual(framer.push("1234"), {
      lines: [],
      overflowed: false,
    });
    NodeAssert.deepStrictEqual(framer.push("56789"), {
      lines: [],
      overflowed: true,
    });
    NodeAssert.deepStrictEqual(framer.push("ok\n"), {
      lines: ["ok"],
      overflowed: false,
    });
  });
});

// @effect-diagnostics nodeBuiltinImport:off

export interface ProviderHostLineFramerPushResult {
  readonly lines: ReadonlyArray<string>;
  readonly overflowed: boolean;
}

export interface ProviderHostLineFramer {
  readonly push: (chunk: string) => ProviderHostLineFramerPushResult;
  readonly clear: () => void;
}

export function makeProviderHostLineFramer(maxLineBytes: number): ProviderHostLineFramer {
  let fragments: Array<string> = [];
  let fragmentBytes = 0;

  const clear = () => {
    fragments = [];
    fragmentBytes = 0;
  };

  const appendFragment = (fragment: string): boolean => {
    if (fragment.length === 0) {
      return true;
    }
    const nextBytes = fragmentBytes + Buffer.byteLength(fragment);
    if (nextBytes > maxLineBytes) {
      clear();
      return false;
    }
    fragments.push(fragment);
    fragmentBytes = nextBytes;
    return true;
  };

  return {
    push: (chunk) => {
      const lines: Array<string> = [];
      let start = 0;
      while (true) {
        const newline = chunk.indexOf("\n", start);
        if (newline < 0) {
          break;
        }
        if (!appendFragment(chunk.slice(start, newline))) {
          return { lines: [], overflowed: true };
        }
        lines.push(fragments.join(""));
        clear();
        start = newline + 1;
      }
      if (!appendFragment(chunk.slice(start))) {
        return { lines: [], overflowed: true };
      }
      return { lines, overflowed: false };
    },
    clear,
  };
}

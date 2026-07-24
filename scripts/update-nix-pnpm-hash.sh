#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package="$root/nix/t3code-unwrapped.nix"

sed -i -E 's#hash = "sha256-[A-Za-z0-9+/=]+";#hash = lib.fakeHash;#' "$package"

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

if nix build "$root#t3code-unwrapped" --no-link 2>"$log"; then
  echo "dependency hash was already valid"
  exit 0
fi

hash="$(
  sed -n \
    -e 's/.*got:[[:space:]]*\\(sha256-[A-Za-z0-9+/=]*\\).*/\\1/p' \
    -e '/^[[:space:]]*got:[[:space:]]*sha256-/s/.*got:[[:space:]]*//p' \
    "$log" | tail -1
)"
if [[ -z "$hash" ]]; then
  cat "$log" >&2
  echo "failed to discover pnpm dependency hash" >&2
  exit 1
fi

sed -i "s#hash = lib.fakeHash;#hash = \"$hash\";#" "$package"
nix build "$root#t3code-unwrapped" --no-link
echo "updated pnpm dependency hash to $hash"

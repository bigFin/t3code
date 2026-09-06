#!/usr/bin/env bash
set -euo pipefail

# Refresh (default) or verify (--check) the fixed-output hash pinning the
# pnpm dependency snapshot in nix/t3code-unwrapped.nix. The hash covers every
# tarball in pnpm-lock.yaml, so any lockfile change invalidates it and the nix
# build fails late with ERR_PNPM_NO_OFFLINE_TARBALL.
#
#   ./scripts/update-nix-pnpm-hash.sh          # refresh + full build check
#   ./scripts/update-nix-pnpm-hash.sh --check  # fetch stage only, for CI
#
# --check restores the file before exiting, so it never leaves a fakeHash
# behind: exit 0 means the pinned hash matches the lockfile, exit 1 means it
# is stale (message tells you to run the refresh).

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package="$root/nix/t3code-unwrapped.nix"

check=false
if [[ "${1:-}" == "--check" ]]; then
  check=true
fi

current="$(sed -n -E 's#^[[:space:]]*hash = "(sha256-[A-Za-z0-9+/=]+)";#\1#p' "$package" | head -1)"
if [[ -z "$current" ]]; then
  echo "no pnpm dependency hash found in $package" >&2
  exit 1
fi

backup="$(mktemp)"
log="$(mktemp)"
cp "$package" "$backup"
cleanup() {
  rm -f "$backup" "$log"
}
restore() {
  cp "$backup" "$package"
  cleanup
}
trap restore EXIT

sed -i -E 's#hash = "sha256-[A-Za-z0-9+/=]+";#hash = lib.fakeHash;#' "$package"

# Build only the dependency-fetch derivation: enough to learn the correct
# hash without compiling the package.
fetch_drv="$(nix eval --raw "$root#t3code-unwrapped.pnpmDeps.drvPath")"
if nix build "$fetch_drv" --no-link 2>"$log"; then
  echo "dependency hash was already valid"
  exit 0
fi

hash="$(
  sed -n \
    -e 's/.*got:[[:space:]]*\(sha256-[A-Za-z0-9+/=]*\).*/\1/p' \
    -e '/^[[:space:]]*got:[[:space:]]*sha256-/s/.*got:[[:space:]]*//p' \
    "$log" | tail -1
)"
if [[ -z "$hash" ]]; then
  cat "$log" >&2
  echo "failed to discover pnpm dependency hash" >&2
  exit 1
fi

if [[ "$check" == true ]]; then
  if [[ "$hash" == "$current" ]]; then
    echo "dependency hash is current"
    exit 0
  fi
  echo "pnpm dependency hash is stale (lockfile changed)." >&2
  echo "Run ./scripts/update-nix-pnpm-hash.sh to refresh it." >&2
  exit 1
fi

sed -i "s#hash = lib.fakeHash;#hash = \"$hash\";#" "$package"
trap cleanup EXIT
nix build "$root#t3code-unwrapped" --no-link
echo "updated pnpm dependency hash to $hash"

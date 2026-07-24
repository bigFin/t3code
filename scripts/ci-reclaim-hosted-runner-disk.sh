#!/usr/bin/env bash
set -euo pipefail

echo "::group::Hosted runner disk before cleanup"
df -h /
echo "::endgroup::"

# GitHub's Ubuntu image includes large SDKs that this repository does not use.
# Keeping the cleanup in-repo avoids another third-party action and leaves enough
# room to materialize the Electron/Nix closure alongside the workspace install.
sudo rm -rf -- \
  /opt/ghc \
  /opt/hostedtoolcache/CodeQL \
  /usr/local/.ghcup \
  /usr/local/lib/android \
  /usr/share/dotnet

sudo docker system prune --all --force --volumes || true
sudo apt-get clean

echo "::group::Hosted runner disk after cleanup"
df -h /
echo "::endgroup::"

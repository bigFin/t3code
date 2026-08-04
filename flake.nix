{
  description = "T3 Code with downstream Codex CLI session support";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      lib = nixpkgs.lib;
      packageSource = lib:
        lib.cleanSourceWith {
          src = self;
          filter = path: _type:
            let
              relative = lib.removePrefix "${toString self}/" (toString path);
            in
              !(relative == ".github"
                || lib.hasPrefix ".github/" relative
                || relative == "DOWNSTREAM.md"
                || relative == "flake.lock"
                || relative == "flake.nix"
                || relative == "nix"
                || lib.hasPrefix "nix/" relative
                || relative == "scripts/ci-reclaim-hosted-runner-disk.sh");
        };
      sourceVersion =
        (builtins.fromJSON (builtins.readFile ./apps/server/package.json)).version;
      sourceVersionCore = builtins.head (lib.splitString "-" sourceVersion);
      sourceVersionParts = lib.splitString "." sourceVersionCore;
      downstreamVersion =
        let
          major = builtins.elemAt sourceVersionParts 0;
          minor = builtins.elemAt sourceVersionParts 1;
          patch = builtins.fromJSON (builtins.elemAt sourceVersionParts 2);
        in
        "${major}.${minor}.${toString (patch + 1)}-bigfin.${toString self.lastModified}";
    in
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ]
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          t3code-unwrapped = pkgs.callPackage ./nix/t3code-unwrapped.nix {
            src = packageSource pkgs.lib;
            version = downstreamVersion;
          };
          t3code = pkgs.callPackage ./nix/t3code.nix {
            inherit t3code-unwrapped;
          };
        in
        {
          formatter = pkgs.nixpkgs-fmt;

          packages = {
            inherit t3code t3code-unwrapped;
            default = t3code;
          };

          apps.default = {
            type = "app";
            program = "${t3code}/bin/t3code-desktop";
          };

          devShells.default = pkgs.mkShell {
            packages = with pkgs; [
              nodejs_24
              pnpm_11
              nixpkgs-fmt
            ];
          };
        }) // {
      overlays.default = final: _prev: {
        t3code-bigfin-unwrapped = final.callPackage ./nix/t3code-unwrapped.nix {
          src = packageSource final.lib;
          version = downstreamVersion;
        };
        t3code-bigfin = final.callPackage ./nix/t3code.nix {
          t3code-unwrapped = final.t3code-bigfin-unwrapped;
        };
      };
    };
}

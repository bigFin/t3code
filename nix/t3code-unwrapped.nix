{ lib
, stdenv
, src
, electron_41
, nodejs_24
, pnpm_11
, fetchPnpmDeps
, pnpmConfigHook
, pnpmBuildHook
, node-gyp
, python3
, cacert
, makeBinaryWrapper
, copyDesktopItems
, makeDesktopItem
, installShellFiles
, version
}:

let
  sourceVersion = (builtins.fromJSON (builtins.readFile ../apps/server/package.json)).version;
in
stdenv.mkDerivation {
  pname = "t3code-bigfin-unwrapped";
  inherit version;
  inherit src;

  strictDeps = true;
  __structuredAttrs = true;

  pnpmDeps = fetchPnpmDeps {
    pnpm = pnpm_11;
    pname = "t3code-bigfin-unwrapped";
    inherit version;
    inherit src;
    fetcherVersion = 4;
    hash = "sha256-cQetEr/DaI+wjvaLcyDrJfecZeyhmV9CrcR7igsEuhg=";
  };

  nativeBuildInputs = [
    installShellFiles
    makeBinaryWrapper
    node-gyp
    nodejs_24
    python3
    pnpmConfigHook
    pnpmBuildHook
    pnpm_11
    cacert
  ] ++ lib.optionals stdenv.hostPlatform.isLinux [ copyDesktopItems ];

  preBuild = ''
    substituteInPlace \
      apps/server/package.json \
      apps/desktop/package.json \
      apps/web/package.json \
      packages/contracts/package.json \
      --replace-fail '"version": "${sourceVersion}"' '"version": "${version}"'

    export npm_config_nodedir=${nodejs_24}
    export ELECTRON_SKIP_BINARY_DOWNLOAD=1
    pnpm rebuild --pending "''${pnpmInstallFlags[@]}" --filter '!@t3tools/monorepo'
  '';

  pnpmBuildScript = "build:desktop";
  postBuild = ''
    node apps/server/scripts/cli.ts pack \
      --out apps/server/t3-server.tgz
    ./node_modules/.bin/vp cache clean
  '';
  dontPatchELF = true;
  dontStrip = true;
  noAuditTmpdir = true;

  installPhase = ''
    runHook preInstall

    mkdir -p "$out"/libexec/t3code/apps/{desktop,server}
    cp -r --no-preserve=mode node_modules "$out"/libexec/t3code
    cp -r --no-preserve=mode apps/server/{node_modules,dist} "$out"/libexec/t3code/apps/server
    install -m444 apps/server/t3-server.tgz \
      "$out"/libexec/t3code/apps/server/t3-server.tgz
    cp -r --no-preserve=mode \
      apps/desktop/{package.json,node_modules,dist-electron} \
      "$out"/libexec/t3code/apps/desktop

    mkdir -p "$out"/libexec/t3code/apps/desktop/prod-resources
    install -m444 assets/prod/black-universal-1024.png \
      "$out"/libexec/t3code/apps/desktop/prod-resources/icon.png
    find "$out"/libexec/t3code -xtype l -delete

    makeWrapper ${lib.getExe nodejs_24} "$out"/bin/t3 \
      --add-flags "$out"/libexec/t3code/apps/server/dist/bin.mjs
    makeWrapper ${lib.getExe electron_41} "$out"/bin/t3code-desktop \
      --add-flags "--password-store=gnome-libsecret" \
      --add-flags "$out"/libexec/t3code/apps/desktop \
      --set T3CODE_REMOTE_T3_PACKAGE_ARCHIVE \
        "$out"/libexec/t3code/apps/server/t3-server.tgz \
      --inherit-argv0

    mkdir -p "$out"/share/icons/hicolor/scalable/apps
    install -m444 assets/prod/logo.svg \
      "$out"/share/icons/hicolor/scalable/apps/t3code.svg

    runHook postInstall
  '';

  desktopItems = [
    (makeDesktopItem {
      name = "t3code";
      desktopName = "T3 Code (bigFin)";
      comment = "Minimal web GUI for coding agents";
      exec = "t3code-desktop %U";
      terminal = false;
      icon = "t3code";
      startupWMClass = "t3code";
      categories = [ "Development" ];
    })
  ];

  meta = {
    description = "T3 Code with downstream Codex CLI session support";
    homepage = "https://github.com/bigFin/t3code";
    license = lib.licenses.mit;
    mainProgram = "t3code-desktop";
    platforms = nodejs_24.meta.platforms;
  };
}

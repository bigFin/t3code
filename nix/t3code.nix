{
  lib,
  symlinkJoin,
  makeBinaryWrapper,
  enableCodex ? false,
  codex,
  enableGitHub ? true,
  gh,
  enableGit ? true,
  git,
  customCssPath ? null,
  transparentWindow ? false,
  t3code-unwrapped,
}:

let
  runtimePackages =
    lib.optionals enableCodex [ codex ]
    ++ lib.optionals enableGitHub [ gh ]
    ++ lib.optionals enableGit [ git ];
in
symlinkJoin {
  pname = "t3code-bigfin";
  inherit (t3code-unwrapped) version;

  paths = [ t3code-unwrapped ];
  nativeBuildInputs = [ makeBinaryWrapper ];

  postBuild = ''
    for program in "$out/bin"/*; do
      wrapperArgs=()
      ${lib.optionalString (runtimePackages != [ ]) ''
        wrapperArgs+=(--prefix PATH : ${lib.escapeShellArg (lib.makeBinPath runtimePackages)})
      ''}
      if [ "$(basename "$program")" = t3code-desktop ]; then
        wrapperArgs+=(--set T3CODE_DESKTOP_LAUNCHER_PATH "$out/bin/t3code-desktop")
        ${lib.optionalString (customCssPath != null) ''
          wrapperArgs+=(--set T3CODE_CUSTOM_CSS ${lib.escapeShellArg customCssPath})
        ''}
        ${lib.optionalString transparentWindow ''
          wrapperArgs+=(--set T3CODE_DESKTOP_TRANSPARENT_WINDOW true)
        ''}
      fi
      if [ "''${#wrapperArgs[@]}" -gt 0 ]; then
        wrapProgram "$program" "''${wrapperArgs[@]}"
      fi
    done
  '';

  passthru.unwrapped = t3code-unwrapped;
  meta = t3code-unwrapped.meta;
}

{ lib
, symlinkJoin
, makeBinaryWrapper
, enableCodex ? false
, codex
, enableGitHub ? true
, gh
, enableGit ? true
, git
, customCssPath ? null
, transparentWindow ? false
, t3code-unwrapped
,
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

  postBuild = lib.optionalString
    (runtimePackages != [ ] || customCssPath != null || transparentWindow)
    ''
      for program in "$out/bin"/*; do
        wrapperArgs=()
        ${lib.optionalString (runtimePackages != [ ]) ''
          wrapperArgs+=(--prefix PATH : ${lib.escapeShellArg (lib.makeBinPath runtimePackages)})
        ''}
        ${lib.optionalString (customCssPath != null || transparentWindow) ''
          if [ "$(basename "$program")" = t3code-desktop ]; then
            ${lib.optionalString (customCssPath != null) ''
              wrapperArgs+=(--set T3CODE_CUSTOM_CSS ${lib.escapeShellArg customCssPath})
            ''}
            ${lib.optionalString transparentWindow ''
              wrapperArgs+=(--set T3CODE_DESKTOP_TRANSPARENT_WINDOW true)
            ''}
          fi
        ''}
        wrapProgram "$program" "''${wrapperArgs[@]}"
      done
    '';

  passthru.unwrapped = t3code-unwrapped;
  meta = t3code-unwrapped.meta;
}

{ lib
, symlinkJoin
, makeBinaryWrapper
, enableCodex ? false
, codex
, enableGitHub ? true
, gh
, enableGit ? true
, git
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

  postBuild = lib.optionalString (runtimePackages != [ ]) ''
    for program in "$out/bin"/*; do
      wrapProgram "$program" \
        --prefix PATH : "${lib.makeBinPath runtimePackages}"
    done
  '';

  passthru.unwrapped = t3code-unwrapped;
  meta = t3code-unwrapped.meta;
}

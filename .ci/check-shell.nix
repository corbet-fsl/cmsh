# Native headers and runtime libraries for Linux verification.
# Includes Tor fixture configure deps (libevent/openssl/zlib) so the pinned
# job-scoped shell links Tor without host apt/sudo; full Tor/browser gates stay.
{ pkgs ? import (builtins.fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/b7c2ada94fe99c15b0dbcf4d11fd7850b957a436.tar.gz";
  sha256 = "sha256-IItrdb7Puk05RqOBWZYFC5X6Wl1sJmCfh5MWVHw5iMM=";
}) {} }:
pkgs.mkShell {
  buildInputs = with pkgs; [
    sqlite
    libevent
    openssl
    zlib
  ];
  nativeBuildInputs = with pkgs; [
    pkg-config
  ];
  shellHook = ''
    export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.sqlite pkgs.libevent pkgs.openssl pkgs.zlib ]}:''${LD_LIBRARY_PATH:-}"
  '';
}

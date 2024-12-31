# shell.nix
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.python3
    pkgs.uv
    pkgs.python3Packages.jupyter
  ];

  # Optional: if you need to install additional Python dependencies, you can use:
  # shellHook = ''
  #   pip install -r requirements.txt
  # '';
}

{
  description = "Arbin CSV parser";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    flake-utils.inputs.nixpkgs.follows = "nixpkgs";
    naersk.url = "github:nix-community/naersk";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, naersk, rust-overlay, ... }@inputs:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ 
          rust-overlay.overlays.default
          (self: super: {
            rustc = self.rust-bin.stable.latest.default;
            cargo = self.rust-bin.stable.latest.default;
          })
        ];
        pkgs = import nixpkgs { inherit system overlays; };
        naersk-lib = naersk.lib."${system}";
        src = ./.;
      in
      rec {
        # nix build
        # packages = rec {
        #   bin = naersk-lib.buildPackage {
        #     pname = "hello-axum";
        #     root = src;
        #     buildInputs = with pkgs; [ pkg-config openssl git ];
        #   };

        #   default = pkgs.symlinkJoin {
        #     name = "hello-axum-${bin.version}";
        #     paths = [ bin ];
        #   };
        # };

        # `nix develop`
        devShell = pkgs.mkShell
          {
            buildInputs = with pkgs; [
              # rust
              cargo
              cargo-edit
              # cargo-generate
              cargo-watch
              rust-analyzer

              # system dependencies
              openssl.dev
              pkg-config
            ] ++ lib.optionals stdenv.isDarwin (with darwin.apple_sdk.frameworks; [
              libiconv
              CoreServices
              SystemConfiguration
            ]);

            RUST_BACKTRACE = 1;
            RUST_LOG = "debug";
          };
      }
    );
}


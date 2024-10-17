# NOTE: Only works with parent directory as build context so ../common/ can be included
# NOTE: cargo-chef doesn'nt work when there are local dependancies like ../common/
FROM rust:1.76 as rust-toolchain

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# # We only pay the installation cost once,
# # it will be cached from the second build onwards
# RUN cargo install cargo-chef
WORKDIR app
# Stop if a command fails
RUN set -eux

# FROM rust-toolchain AS planner
# COPY . .
# RUN cargo chef prepare --recipe-path recipe.json

FROM rust-toolchain AS builder
# COPY --from=planner /app/recipe.json recipe.json
# # Build dependencies - this is the caching Docker layer!
# RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin client

FROM debian:bookworm-slim AS runtime
WORKDIR app
COPY --from=builder /app/target/release/client /usr/local/bin
ENTRYPOINT ["/usr/local/bin/client"]

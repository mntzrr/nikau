# Builds an image containing the binary and little else.

# Builder image with initial project for Cargo.* deps to build in

FROM docker.io/library/rust:slim
RUN apt-get update \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists /var/cache/apt/archives \
  && cargo --version

COPY . /monux
RUN cd /monux && cargo build --release

# Release image: copy executable from builder
# Debian version needs to match builder image to avoid linker issues.

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists /var/cache/apt/archives
COPY --from=0 /monux/target/release/monux /monux
RUN chmod +x /monux && /monux --version

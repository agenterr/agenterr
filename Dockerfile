# Built by goreleaser (see .goreleaser.yml, `dockers:`), not by `docker
# build` directly: goreleaser already has the right per-arch `agenterr`
# binary in the build context, plus a `ca-certificates.crt` staged next to
# it by the release workflow (see .github/workflows/release.yml) — this
# Dockerfile only assembles the final scratch image from those two files.
#
# Under Model 3 (see docs/architecture.md) this image is what the hosted
# cloud offering runs — nothing "extra" here is optional.
FROM scratch

COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY agenterr /agenterr

VOLUME /data
ENV AGENTERR_DB=/data/agenterr.db
EXPOSE 3617

ENTRYPOINT ["/agenterr"]

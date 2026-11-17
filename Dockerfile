# syntax=docker/dockerfile:1
FROM ghcr.io/dimensiondev/hub-monorepo/shuttle:sha-e5fbaa4

ARG SHUTTLE_REVISION=unknown
LABEL org.opencontainers.image.revision=$SHUTTLE_REVISION

# The base supplies Node 24 and the pinned library dependencies.
# Overlay the maintained app, including its business projections.
COPY packages/shuttle/src/example-app/ /src/packages/shuttle/src/example-app/
WORKDIR /src/packages/shuttle
CMD ["yarn", "start", "start"]

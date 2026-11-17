# syntax=docker/dockerfile:1
FROM ghcr.io/dimensiondev/hub-monorepo/shuttle:sha-e5fbaa4

# The base supplies Node 24 and the pinned library dependencies.
# Keep the locally developed raw-capture app in the deployed image.
COPY packages/shuttle/src/example-app/ /src/packages/shuttle/src/example-app/
WORKDIR /src/packages/shuttle
CMD ["yarn", "start", "start"]

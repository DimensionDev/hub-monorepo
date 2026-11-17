# syntax=docker/dockerfile:1
# Production image for the shuttle app, built entirely from this repository so that
# library changes (packages/shuttle/src/shuttle) ship alongside the app and projections.
ARG NODE_VERSION=24
FROM node:${NODE_VERSION}-bookworm

ARG SHUTTLE_REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/DimensionDev/hub-monorepo"
LABEL org.opencontainers.image.description="Shuttle app with inline business projections"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.revision=$SHUTTLE_REVISION

# node-gyp and native addons built during `yarn install`.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      python3 \
      make \
      g++ \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Build from the local checkout; .dockerignore keeps .env files and build output out.
COPY . .

RUN if ! command -v yarn >/dev/null 2>&1; then npm install -g yarn@1.22.19; fi \
    && yarn install --frozen-lockfile \
    && yarn --cwd packages/core build \
    && yarn --cwd packages/hub-nodejs build \
    && yarn --cwd packages/shuttle build

# `yarn start` runs the app with the shuttle package's own tsx dependency.
WORKDIR /src/packages/shuttle

CMD ["yarn", "start", "start"]

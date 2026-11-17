# syntax=docker/dockerfile:1
# Production image for the shuttle app, built entirely from this repository so that
# library changes (packages/shuttle/src/shuttle) ship alongside the app and projections.
#
# Layer order matters for build time on the deploy server: dependencies are installed from
# the manifests alone, so code-only changes reuse the cached `yarn install` layer.
ARG NODE_VERSION=24
FROM node:${NODE_VERSION}-bookworm

LABEL org.opencontainers.image.source="https://github.com/DimensionDev/hub-monorepo"
LABEL org.opencontainers.image.description="Shuttle app with inline business projections"
LABEL org.opencontainers.image.licenses="MIT"

# node-gyp and native addons built during `yarn install`.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      python3 \
      make \
      g++ \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN if ! command -v yarn >/dev/null 2>&1; then npm install -g yarn@1.22.19; fi

WORKDIR /src

# 1. Dependencies: only the workspace manifests and the lockfile. Keep this list in sync with
#    packages/* (yarn --frozen-lockfile fails loudly if a workspace manifest is missing).
COPY package.json yarn.lock ./
COPY packages/biome-config-custom/package.json packages/biome-config-custom/
COPY packages/core/package.json packages/core/
COPY packages/hub-nodejs/package.json packages/hub-nodejs/
COPY packages/hub-web/package.json packages/hub-web/
COPY packages/shuttle/package.json packages/shuttle/
RUN yarn install --frozen-lockfile

# 2. Sources and build (core → hub-nodejs → shuttle). .dockerignore keeps .env files,
#    node_modules and build output out of the context.
COPY . .
RUN yarn --cwd packages/core build \
    && yarn --cwd packages/hub-nodejs build \
    && yarn --cwd packages/shuttle build

# Declared last: a build arg invalidates the cache of every RUN after its ARG line.
ARG SHUTTLE_REVISION=unknown
LABEL org.opencontainers.image.revision=$SHUTTLE_REVISION
ENV SHUTTLE_REVISION=$SHUTTLE_REVISION

# `yarn start` runs the app with the shuttle package's own tsx dependency.
WORKDIR /src/packages/shuttle

CMD ["yarn", "start", "start"]

# syntax=docker/dockerfile:1
#
# Builds the shuttle sync worker from the pinned @farcaster/shuttle release.
# The upstream package is a library; the example app in src/example-app is used
# here as a runnable starting point. Copy that example into your own app when
# you add a MessageHandler for business logic.
ARG NODE_VERSION=24
FROM node:${NODE_VERSION}-bookworm

ARG HUB_MONOREPO_REF=@farcaster/shuttle@1.0.3
ARG HUB_MONOREPO_URL=https://github.com/farcasterxyz/hub-monorepo.git

RUN apt-get update \
 && apt-get install -y --no-install-recommends git python3 make g++ ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --depth 1 --branch "${HUB_MONOREPO_REF}" "${HUB_MONOREPO_URL}" .

# Same build order as the official quickstart:
# packages/core -> packages/hub-nodejs -> packages/shuttle.
RUN yarn install --frozen-lockfile \
 && yarn --cwd packages/core build \
 && yarn --cwd packages/hub-nodejs build \
 && yarn --cwd packages/shuttle build

# The upstream package's `start` script runs `tsx src/example-app/app.ts`, but
# tsx is not a dependency of the shuttle package (the example app is not part of
# the yarn workspace). Install it globally so `yarn start ...` resolves it.
RUN npm install -g tsx@3.12.5

WORKDIR /src/packages/shuttle
CMD ["yarn", "start", "start"]

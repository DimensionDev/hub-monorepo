# Production image for the shuttle example app, built from this repository.
ARG NODE_VERSION=24
FROM node:${NODE_VERSION}-bookworm

LABEL org.opencontainers.image.source="https://github.com/DimensionDev/hub-monorepo"
LABEL org.opencontainers.image.description="Shuttle example app from the DimensionDev hub-monorepo"
LABEL org.opencontainers.image.licenses="MIT"

# Build/runtime requirements: git is useful for dependency resolution, the rest
# covers node-gyp and any native addons built during `yarn install`.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      python3 \
      make \
      g++ \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Build straight from the local checkout instead of cloning the repository.
COPY . .

# The official node image ships yarn classic, but bootstrap it if it is missing.
RUN if ! command -v yarn >/dev/null 2>&1; then npm install -g yarn@1.22.19; fi \
    && yarn install --frozen-lockfile \
    && yarn --cwd packages/core build \
    && yarn --cwd packages/hub-nodejs build \
    && yarn --cwd packages/shuttle build

# The shuttle start script is executed with tsx from `yarn start start`.
RUN npm install -g tsx@3.12.5

WORKDIR /src/packages/shuttle

CMD ["yarn", "start", "start"]

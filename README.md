# Hub Monorepo

This monorepo contains packages used to communicate with a Farcaster Hub.

## Getting Started

See [CONTRIBUTING.md](./CONTRIBUTING.md) to set up your developer environment and learn about how to contribute.

## Code Organization

The repository is a monorepo with a primary application in the `/apps/` folder that imports several packages `/packages/`. It is written primarily in [Typescript](https://www.typescriptlang.org/) and uses [Yarn](https://yarnpkg.com/) to orchestrate tasks and [TurboRepo](https://turbo.build/) as its build system. Some performance intensive code is written in Rust and compiled with Cargo.

### Packages

| Package Name                                      | Description                                        |
| ------------------------------------------------- | -------------------------------------------------- |
| [@dimensiondev/shuttle](./packages/shuttle)       | A package that streams Hubble events to Postgres   |
| [@dimensiondev/hub-nodejs](./packages/hub-nodejs) | A Node.js client library for Hubble                |
| [@dimensiondev/hub-web](./packages/hub-web)       | A Browser client library for Hubble                |
| [@dimensiondev/core](./packages/core)             | Shared code between all packages                   |

Packages are published to the [GitHub Packages npm registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-npm-registry) under the `@dimensiondev` scope.

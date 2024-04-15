# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/eigerco/beetswap/releases/tag/v0.1.0) - 2024-04-15

### Added
- [**breaking**] Use RPITIT for `Multihasher` and remove `async-trait` ([#39](https://github.com/eigerco/beetswap/pull/39))
- Add bitswap server ([#27](https://github.com/eigerco/beetswap/pull/27))
- Add helpers for MultihasherError ([#25](https://github.com/eigerco/beetswap/pull/25))
- Allow user to reject a multihash via Multihasher ([#23](https://github.com/eigerco/beetswap/pull/23))
- Allow `BitswapQueryId` to be used in `BTreeMap` ([#15](https://github.com/eigerco/beetswap/pull/15))
- Send full wantlist every 30 seconds ([#7](https://github.com/eigerco/beetswap/pull/7))
- Allow user to register its own hashers ([#6](https://github.com/eigerco/beetswap/pull/6))

### Fixed
- Fix client sending state mechanism that was blocking the executor ([#36](https://github.com/eigerco/beetswap/pull/36))
- Fix broken link in readme ([#34](https://github.com/eigerco/beetswap/pull/34))
- protocol_prefix was returning an error on correct input ([#22](https://github.com/eigerco/beetswap/pull/22))
- Do not reset peer state on new established connection ([#16](https://github.com/eigerco/beetswap/pull/16))
- *(ci)* Do not install protobuf compiler ([#4](https://github.com/eigerco/beetswap/pull/4))
- Send full list on the first request ([#1](https://github.com/eigerco/beetswap/pull/1))

### Other
- Upgrade to blockstore 0.5 ([#43](https://github.com/eigerco/beetswap/pull/43))
- *(ci)* introduce a release plz workflow ([#41](https://github.com/eigerco/beetswap/pull/41))
- Split `futures` dependencies ([#40](https://github.com/eigerco/beetswap/pull/40))
- Upgrade to blockstore 0.4.0 ([#38](https://github.com/eigerco/beetswap/pull/38))
- Removed unneeded Send/Sync constrains ([#37](https://github.com/eigerco/beetswap/pull/37))
- Add doc requirement to CI, add missing docs ([#28](https://github.com/eigerco/beetswap/pull/28))
- Remove `Bitswap` prefix from all names ([#21](https://github.com/eigerco/beetswap/pull/21))
- Add more docs and comments ([#19](https://github.com/eigerco/beetswap/pull/19))
- Rename project to beetswap ([#20](https://github.com/eigerco/beetswap/pull/20))
- *(deps)* Split libp2p and add CI test for minimal versions ([#18](https://github.com/eigerco/beetswap/pull/18))
- Upgrade dependencies ([#5](https://github.com/eigerco/beetswap/pull/5))
- Add tests ([#2](https://github.com/eigerco/beetswap/pull/2))
- Add CI ([#3](https://github.com/eigerco/beetswap/pull/3))
- Move WantlistState in ClietBehaviour and cleanup some code
- Implement bitswap client
- Add license

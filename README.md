# Beetswap

**Beetswap** is a Rust-based implementation of the [`Bitswap`] protocol for the [`libp2p`] networking stack.
Bitswap is a protocol developed for the exchange of blocks between peers. It plays a critical role in data distribution, ensuring that peers can request and receive the blocks they need from other peers in the network. 

Check out the [examples](examples).

## Key Features

- **Efficient Block Exchange**: Implements the Bitswap protocol to ensure efficient transfer of blocks between peers.
- **Libp2p Integration**: Designed to integrate smoothly with libp2p, allowing it to be used in a wide range of decentralized applications.
- **Rust Safety and Performance**: Takes advantage of Rust's memory safety guarantees and performance optimizations, making it a robust choice for networked applications.

## Contributing

We welcome contributions! Please fork the repository and submit a pull request.

## License

Beetswap is licensed under the Apache 2.0 License. See the [LICENSE](./LICENSE) file for more details.

## About [Eiger](https://www.eiger.co)

We are engineers. We contribute to various ecosystems by building low level implementations and core components.

Contact us at hello@eiger.co
Follow us on [X/Twitter](https://x.com/eiger_co)

[`Bitswap`]: https://specs.ipfs.tech/bitswap-protocol/
[`libp2p`]: https://docs.rs/libp2p

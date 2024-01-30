use std::sync::Arc;

use blockstore::Blockstore;

use crate::client::{ClientBehaviour, ClientConfig};
use crate::multihasher::{Multihasher, MultihasherTable};
use crate::utils::stream_protocol;
use crate::{BitswapBehaviour, BitswapError, Result};

/// Builder for [`BitswapBehaviour`].
///
/// # Example
///
/// ```rust,no_run
/// # use blockstore::InMemoryBlockstore;
/// # use beetswap::BitswapBehaviour;
/// # fn new() -> BitswapBehaviour<64, InMemoryBlockstore<64>> {
/// BitswapBehaviour::builder(InMemoryBlockstore::new())
///     .build()
/// # }
pub struct BitswapBehaviourBuilder<const S: usize, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    protocol_prefix: Option<String>,
    blockstore: B,
    client: ClientConfig,
    multihasher: MultihasherTable<S>,
}

impl<const S: usize, B> BitswapBehaviourBuilder<S, B>
where
    B: Blockstore + Send + Sync + 'static,
{
    /// Creates a new builder for [`BitswapBehaviour`].
    pub(crate) fn new(blockstore: B) -> Self {
        BitswapBehaviourBuilder {
            protocol_prefix: None,
            blockstore,
            client: ClientConfig {
                set_send_dont_have: true,
            },
            multihasher: MultihasherTable::<S>::new(),
        }
    }

    /// Set a prefix on the protocol name.
    ///
    /// The prefix will be added on `/ipfs/bitswap/1.2.0`.
    ///
    /// # Errors
    ///
    /// This function will return an error if `prefix` does not start with a forward slash (`/`).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use blockstore::InMemoryBlockstore;
    /// # use beetswap::BitswapBehaviour;
    /// # fn new() -> beetswap::Result<BitswapBehaviour<64, InMemoryBlockstore<64>>> {
    /// #   Ok(
    /// BitswapBehaviour::builder(InMemoryBlockstore::new())
    ///     .protocol_prefix("/celestia/celestia")?
    ///     .build()
    /// #   )
    /// # }
    /// ```
    pub fn protocol_prefix(mut self, prefix: &str) -> Result<Self> {
        if prefix.starts_with('/') {
            return Err(BitswapError::InvalidProtocolPrefix(prefix.to_owned()));
        }

        self.protocol_prefix = Some(prefix.to_owned());
        Ok(self)
    }

    /// Client will set `send_dont_have` flag on each query (enabled by default).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use blockstore::InMemoryBlockstore;
    /// # use beetswap::BitswapBehaviour;
    /// # fn new() -> BitswapBehaviour<64, InMemoryBlockstore<64>> {
    /// BitswapBehaviour::builder(InMemoryBlockstore::new())
    ///     .client_set_send_dont_have(false)
    ///     .build()
    /// # }
    pub fn client_set_send_dont_have(mut self, enable: bool) -> Self {
        self.client.set_send_dont_have = enable;
        self
    }

    /// Register an extra [`Multihasher`].
    ///
    /// Every registration adds new hasher to [`BitswapBehaviour`]. Hashers are used to
    /// reconstruct the [`Cid`] from the received data. `BitswapBehaviour` will try them
    /// in the reverse order they were registered until one successfully constructs
    /// [`Multihash`].
    ///
    /// By default `BitswapBehaviourBuilder` is pre-loaded with [`StandardMultihasher`].
    ///
    /// [`StandardMultihasher`]: crate::multihasher::StandardMultihasher
    /// [`Cid`]: cid::CidGeneric
    /// [`Multihash`]: libp2p_core::multihash::Multihash
    pub fn register_multihasher<M>(mut self, multihasher: M) -> Self
    where
        M: Multihasher<S> + Send + Sync + 'static,
    {
        self.multihasher.register(multihasher);
        self
    }

    /// Build a [`BitswapBehaviour`].
    pub fn build(self) -> BitswapBehaviour<S, B> {
        let blockstore = Arc::new(self.blockstore);
        let multihasher = Arc::new(self.multihasher);
        let protocol_prefix = self.protocol_prefix.as_deref();

        BitswapBehaviour {
            protocol: stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")
                .expect("prefix checked by BitswapBehaviourBuilder::protocol_prefix"),
            client: ClientBehaviour::new(self.client, blockstore, multihasher, protocol_prefix),
        }
    }
}

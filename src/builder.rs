use std::sync::Arc;

use blockstore::Blockstore;

use crate::client::{ClientBehaviour, ClientConfig};
use crate::multihasher::{Multihasher, MultihasherTable};
use crate::utils::stream_protocol;
use crate::{BitswapBehaviour, Result};

/// Builder for [`BitswapBehaviour`].
///
/// # Example
///
/// ```rust,no_run
/// # use blockstore::InMemoryBlockstore;
/// # use bitmingle::BitswapBehaviour;
/// # fn main() -> bitmingle::Result<()> {
/// BitswapBehaviour::<64, _>::builder(InMemoryBlockstore::<64>::new())
///     .build()?;
/// #   Ok(())
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
    /// The prefix will be added on `/ipfs/bitswap/1.2.0` and it must start with `/`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use blockstore::InMemoryBlockstore;
    /// # use bitmingle::BitswapBehaviour;
    /// # fn main() -> bitmingle::Result<()> {
    /// BitswapBehaviour::<64, _>::builder(InMemoryBlockstore::<64>::new())
    ///     .protocol_prefix("/celestia/celestia")
    ///     .build()?;
    /// #   Ok(())
    /// # }
    /// ```
    pub fn protocol_prefix(mut self, prefix: &str) -> Self {
        self.protocol_prefix = Some(prefix.to_owned());
        self
    }

    /// Client will set `send_dont_have` flag on each query.
    pub fn client_set_send_dont_have(mut self, enable: bool) -> Self {
        self.client.set_send_dont_have = enable;
        self
    }

    /// Register extra [`Multihasher`].
    ///
    /// Every registration adds new hasher to `BitswapBehavior`. Hashers are used to reconstruct the `Cid` from the received data.
    /// `BitswapBehavior` will try them in the reverse order they were registered until some successfully constructs `Multihash`.
    /// By default `BitswapBehaviourBuilder` is pre-loaded with [`StandardMultihasher`].

    ///
    /// [`StandardMultihasher`]: crate::multihasher::StandardMultihasher
    pub fn register_multihasher<M>(mut self, multihasher: M) -> Self
    where
        M: Multihasher<S> + Send + Sync + 'static,
    {
        self.multihasher.register(multihasher);
        self
    }

    /// Build a [`BitswapBehaviour`].
    pub fn build(self) -> Result<BitswapBehaviour<S, B>> {
        let blockstore = Arc::new(self.blockstore);
        let multihasher = Arc::new(self.multihasher);
        let protocol_prefix = self.protocol_prefix.as_deref();

        Ok(BitswapBehaviour {
            protocol: stream_protocol(protocol_prefix, "/ipfs/bitswap/1.2.0")?,
            client: ClientBehaviour::new(self.client, blockstore, multihasher, protocol_prefix)?,
        })
    }
}

use std::collections::VecDeque;
use std::fmt;

use async_trait::async_trait;
use libp2p_core::multihash::Multihash;
use multihash_codetable::MultihashDigest;

use crate::utils::convert_multihash;

/// Errors that can be produced by [`Multihasher`] trait.
#[derive(Debug, thiserror::Error)]
pub enum MultihasherError {
    /// [`Multihasher`] can not handle the specified multihash code.
    #[error("Unknown multihash code")]
    UnknownMultihashCode,

    /// Bigger [`Multihash`] is needed.
    ///
    /// Maximum allowed size of [`Multihash`] is specified as generic const when
    /// [`Behaviour`] is constructed.
    ///
    /// [`Behaviour`]: crate::Behaviour
    #[error("Invalid multihash size")]
    InvalidMultihashSize,

    /// Custom error.
    ///
    /// This error will be logged and the specified `input` will be ignored.
    #[error("Hashing failure: {0}")]
    Custom(String),

    /// Custom error that causes [`Stream`] to close.
    ///
    /// This error will be logged and the [`Stream`] which delivered the
    /// `input` will be closed.
    ///
    /// [`Stream`]: libp2p_swarm::Stream
    #[error("Fatal hashing failure: {0}")]
    CustomFatal(String),
}

/// Trait for producing a custom [`Multihash`].
#[async_trait]
pub trait Multihasher<const S: usize> {
    /// Hash the `input` based on the `multihash_code`.
    ///
    /// If this `Multihasher` can not handle the specified `multihash_code`, then
    /// [`MultihasherError::UnknownMultihashCode`] must be returned. In this
    /// case hashing will be re-tried with the next `Multihasher`. For more info check
    /// [`BehaviourBuilder::register_multihasher`].
    ///
    /// [`BehaviourBuilder::register_multihasher`]: crate::BehaviourBuilder::register_multihasher
    async fn hash(
        &self,
        multihash_code: u64,
        input: &[u8],
    ) -> Result<Multihash<S>, MultihasherError>;
}

/// [`Multihasher`] that uses [`multihash_codetable::Code`]
pub struct StandardMultihasher;

#[async_trait]
impl<const S: usize> Multihasher<S> for StandardMultihasher {
    async fn hash(
        &self,
        multihash_code: u64,
        input: &[u8],
    ) -> Result<Multihash<S>, MultihasherError> {
        let hasher = multihash_codetable::Code::try_from(multihash_code)
            .map_err(|_| MultihasherError::UnknownMultihashCode)?;

        let hash = hasher.digest(input);

        convert_multihash(&hash).ok_or(MultihasherError::InvalidMultihashSize)
    }
}

pub(crate) struct MultihasherTable<const S: usize> {
    multihashers: VecDeque<Box<dyn Multihasher<S> + Send + Sync + 'static>>,
}

impl<const S: usize> fmt::Debug for MultihasherTable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MultihasherTable { .. }")
    }
}

impl<const S: usize> MultihasherTable<S> {
    pub(crate) fn new() -> Self {
        let mut table = MultihasherTable {
            multihashers: VecDeque::new(),
        };

        table.register(StandardMultihasher);

        table
    }

    pub(crate) fn register<M>(&mut self, multihasher: M)
    where
        M: Multihasher<S> + Send + Sync + 'static,
    {
        self.multihashers.push_front(Box::new(multihasher));
    }

    pub(crate) async fn hash(
        &self,
        multihash_code: u64,
        input: &[u8],
    ) -> Result<Multihash<S>, MultihasherError> {
        for multihasher in &self.multihashers {
            match multihasher.hash(multihash_code, input).await {
                Ok(hash) => return Ok(hash),
                // `multihash_code` cannot be handled by this multihasher
                // so we move to the next one.
                Err(MultihasherError::UnknownMultihashCode) => continue,
                Err(e) => return Err(e),
            }
        }

        // Reaching this point means there isn't any registered multihasher
        // that can handle `multihash_code`.
        Err(MultihasherError::UnknownMultihashCode)
    }
}

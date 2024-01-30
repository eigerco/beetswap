use std::collections::VecDeque;
use std::fmt;

use libp2p_core::multihash::Multihash;
use multihash_codetable::MultihashDigest;

use crate::utils::convert_multihash;

/// Trait for producing a custom [`Multihash`].
pub trait Multihasher<const S: usize> {
    fn digest(&self, multihash_code: u64, input: &[u8]) -> Option<Multihash<S>>;
}

/// [`Multihasher`] that uses [`multihash_codetable::Code`]
pub struct StandardMultihasher;

impl<const S: usize> Multihasher<S> for StandardMultihasher {
    fn digest(&self, multihash_code: u64, input: &[u8]) -> Option<Multihash<S>> {
        let hash = multihash_codetable::Code::try_from(multihash_code)
            .ok()?
            .digest(input);
        convert_multihash(&hash)
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

    pub(crate) fn digest(&self, multihash_code: u64, input: &[u8]) -> Option<Multihash<S>> {
        for multihasher in &self.multihashers {
            if let Some(hash) = multihasher.digest(multihash_code, input) {
                return Some(hash);
            }
        }

        None
    }
}

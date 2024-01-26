use cid::CidGeneric;
use libp2p_core::multihash::Multihash;
use libp2p_swarm::StreamProtocol;

use crate::{BitswapError, Result};

/// Helper utility that converts `CidGeneric<INPUT_SIZE>` to `CidGeneric<OUTPUT_SIZE>`
pub fn convert_cid<const INPUT_S: usize, const OUTPUT_S: usize>(
    cid: &CidGeneric<INPUT_S>,
) -> Option<CidGeneric<OUTPUT_S>> {
    let hash = convert_multihash(cid.hash())?;
    CidGeneric::new(cid.version(), cid.codec(), hash).ok()
}

/// Helper utility that converts `Multihash<INPUT_SIZE>` to `Multihash<OUTPUT_SIZE>`
pub fn convert_multihash<const INPUT_S: usize, const OUTPUT_S: usize>(
    hash: &Multihash<INPUT_S>,
) -> Option<Multihash<OUTPUT_S>> {
    Multihash::<OUTPUT_S>::wrap(hash.code(), hash.digest()).ok()
}

pub(crate) fn stream_protocol(
    prefix: Option<&str>,
    protocol: &'static str,
) -> Result<StreamProtocol> {
    match prefix {
        Some(prefix) => StreamProtocol::try_from_owned(format!("{prefix}{protocol}"))
            .map_err(|_| BitswapError::InvalidProtocolPrefix(prefix.to_owned())),
        None => Ok(StreamProtocol::new(protocol)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::RAW_CODEC;
    use multihash_codetable::{Code, MultihashDigest};

    #[test]
    fn convert_cid_len() {
        let hash = Code::Sha2_256.digest(&[]);
        let cid = CidGeneric::<64>::new_v1(RAW_CODEC, hash);

        assert!(convert_cid::<64, 32>(&cid).is_some());
        assert!(convert_cid::<64, 31>(&cid).is_none());
    }

    #[test]
    fn convert_multihash_len() {
        let hash = Code::Sha2_256.digest(&[]);

        assert!(convert_multihash::<64, 32>(&hash).is_some());
        assert!(convert_multihash::<64, 31>(&hash).is_none());
    }
}

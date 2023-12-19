use cid::CidGeneric;
use libp2p::StreamProtocol;
use multihash::Multihash;

use crate::{BitswapError, Result};

pub(crate) fn convert_cid<const INPUT_S: usize, const OUTPUT_S: usize>(
    cid: &CidGeneric<INPUT_S>,
) -> Option<CidGeneric<OUTPUT_S>> {
    let hash = convert_multihash(cid.hash())?;
    CidGeneric::new(cid.version(), cid.codec(), hash).ok()
}

pub(crate) fn convert_multihash<const INPUT_S: usize, const OUTPUT_S: usize>(
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
            .map_err(|_| BitswapError::InvalidProtocolPrefix),
        None => Ok(StreamProtocol::new(protocol)),
    }
}

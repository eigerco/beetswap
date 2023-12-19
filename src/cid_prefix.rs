use cid::{CidGeneric, Version};
use multihash_codetable::{Code, MultihashDigest};

use crate::utils::convert_multihash;

const DAG_PB: u64 = 0x70;
const SHA2_256: u64 = 0x12;
const SHA2_256_SIZE: usize = 0x20;

pub(crate) struct CidPrefix {
    version: Version,
    codec: u64,
    multihash_code: u64,
    multihash_size: usize,
}

impl CidPrefix {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<CidPrefix> {
        let (raw_version, rest) = unsigned_varint::decode::u64(bytes).ok()?;
        let (codec, rest) = unsigned_varint::decode::u64(rest).ok()?;

        // CIDv0 is a naked multihash that with fixed code and size
        if raw_version == SHA2_256 && codec == SHA2_256_SIZE as u64 {
            return Some(CidPrefix {
                version: Version::V0,
                codec: DAG_PB,
                multihash_code: SHA2_256,
                multihash_size: SHA2_256_SIZE,
            });
        }

        let version = Version::try_from(raw_version).ok()?;
        let (multihash_code, rest) = unsigned_varint::decode::u64(rest).ok()?;
        let (multihash_size, _rest) = unsigned_varint::decode::usize(rest).ok()?;

        Some(CidPrefix {
            version,
            codec,
            multihash_code,
            multihash_size,
        })
    }

    pub(crate) fn to_cid<const S: usize>(&self, data: &[u8]) -> Option<CidGeneric<S>> {
        if self.multihash_size > S {
            return None;
        }

        // TODO: Provide a way for user to have additional codetable
        let hash = Code::try_from(self.multihash_code).ok()?.digest(data);
        let hash = convert_multihash(&hash)?;

        CidGeneric::new(self.version, self.codec, hash).ok()
    }
}

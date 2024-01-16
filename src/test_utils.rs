use std::future::poll_fn;
use std::task::{Context, Poll};

use cid::Cid;
use futures::future::Either;
use multihash_codetable::{Code, MultihashDigest};

pub const RAW_CODEC: u64 = 0x55;

pub fn cid_of_data(data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(RAW_CODEC, hash)
}

pub(crate) async fn select_poll_fn<T1, F1, T2, F2>(mut f1: F1, mut f2: F2) -> Either<T1, T2>
where
    F1: FnMut(&mut Context<'_>) -> Poll<T1>,
    F2: FnMut(&mut Context<'_>) -> Poll<T2>,
{
    poll_fn(|cx| {
        if let Poll::Ready(val) = f1(cx).map(Either::Left) {
            return Poll::Ready(val);
        }

        f2(cx).map(Either::Right)
    })
    .await
}

pub(crate) async fn poll_fn_once<T, F>(mut f: F) -> Option<T>
where
    F: FnMut(&mut Context<'_>) -> Poll<T>,
{
    poll_fn(|cx| match f(cx) {
        Poll::Ready(val) => Poll::Ready(Some(val)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}

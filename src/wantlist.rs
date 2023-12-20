use std::collections::hash_map;

use cid::CidGeneric;
use fnv::{FnvHashMap, FnvHashSet};

use crate::message::{new_cancel_entry, new_want_block_entry, new_want_have_entry};
use crate::proto::message::mod_Message::Wantlist as ProtoWantlist;

#[derive(Debug)]
pub(crate) struct Wantlist<const S: usize> {
    cids: FnvHashSet<CidGeneric<S>>,
    revision: u64,
}

impl<const S: usize> Wantlist<S> {
    pub(crate) fn new() -> Self {
        Wantlist {
            cids: FnvHashSet::default(),
            revision: 0,
        }
    }

    pub(crate) fn insert(&mut self, cid: CidGeneric<S>) -> bool {
        if self.cids.insert(cid) {
            self.revision += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn remove(&mut self, cid: &CidGeneric<S>) -> bool {
        if self.cids.remove(cid) {
            self.revision += 1;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub(crate) struct WantlistState<const S: usize> {
    req_state: FnvHashMap<CidGeneric<S>, WantReqState>,
    force_update: bool,
    synced_revision: u64,
}

#[derive(Debug, Clone, Copy)]
enum WantReqState {
    SentWantHave,
    GotHave,
    GotDontHave,
    SentWantBlock,
    GotBlock,
}

impl<const S: usize> WantlistState<S> {
    pub(crate) fn new() -> Self {
        WantlistState {
            req_state: FnvHashMap::default(),
            force_update: true,
            synced_revision: 0,
        }
    }

    pub(crate) fn is_updated(&self, wantlist: &Wantlist<S>) -> bool {
        !self.force_update && self.synced_revision == wantlist.revision
    }

    pub(crate) fn got_have(&mut self, cid: &CidGeneric<S>) {
        self.req_state
            .entry(cid.to_owned())
            .and_modify(|state| *state = WantReqState::GotHave);
        self.force_update = true;
    }

    pub(crate) fn got_dont_have(&mut self, cid: &CidGeneric<S>) {
        self.req_state
            .entry(cid.to_owned())
            .and_modify(|state| *state = WantReqState::GotDontHave);
    }

    pub(crate) fn got_block(&mut self, cid: &CidGeneric<S>) {
        self.req_state
            .entry(cid.to_owned())
            .and_modify(|state| *state = WantReqState::GotBlock);
    }

    pub(crate) fn generate_proto_full(
        &mut self,
        wantlist: &Wantlist<S>,
        set_send_dont_have: bool,
    ) -> ProtoWantlist {
        // Remove canceled requests or received blocks
        self.req_state.retain(|cid, _| wantlist.cids.contains(cid));

        // Add new entries
        for cid in &wantlist.cids {
            self.req_state
                .entry(cid.to_owned())
                .or_insert(WantReqState::SentWantHave);
        }

        let mut entries = Vec::new();

        for (cid, req_state) in self.req_state.iter_mut() {
            match *req_state {
                WantReqState::SentWantHave => {
                    entries.push(new_want_have_entry(cid, set_send_dont_have));
                }
                WantReqState::GotHave => {
                    entries.push(new_want_block_entry(cid, set_send_dont_have));
                    *req_state = WantReqState::SentWantBlock;
                }
                WantReqState::GotDontHave => {
                    // Nothing to request
                }
                WantReqState::SentWantBlock => {
                    entries.push(new_want_block_entry(cid, set_send_dont_have));
                }
                WantReqState::GotBlock => {
                    // Nothing to request
                }
            }
        }

        ProtoWantlist {
            entries,
            full: true,
        }
    }

    pub(crate) fn generate_proto_update(
        &mut self,
        wantlist: &Wantlist<S>,
        set_send_dont_have: bool,
    ) -> ProtoWantlist {
        if self.is_updated(wantlist) {
            return ProtoWantlist::default();
        }

        let mut entries = Vec::new();
        let mut removed = Vec::new();

        // Update existing entries
        for (cid, req_state) in self.req_state.iter_mut() {
            match (wantlist.cids.contains(cid), *req_state) {
                // If CID is not in wantlist but we got the block, remove it
                (false, WantReqState::GotBlock) => {
                    removed.push(cid.to_owned());
                }

                // If CID is not in wantlist, cancel it
                (false, _) => {
                    removed.push(cid.to_owned());
                    entries.push(new_cancel_entry(cid));
                }

                // WantHave was requested. We need a response to proceed further.
                (true, WantReqState::SentWantHave) => {}

                (true, WantReqState::GotHave) => {
                    entries.push(new_want_block_entry(cid, set_send_dont_have));
                    *req_state = WantReqState::SentWantBlock;
                }

                // Server responsed with DontHave. We have nothing else to do.
                (true, WantReqState::GotDontHave) => {}

                // Block was requested
                (true, WantReqState::SentWantBlock) => {}

                // Block was received
                (true, WantReqState::GotBlock) => {}
            }
        }

        // Remove canceled requests or received blocks
        for cid in removed {
            self.req_state.remove(&cid);
        }

        // Add new entries
        for cid in &wantlist.cids {
            if let hash_map::Entry::Vacant(state_entry) = self.req_state.entry(cid.to_owned()) {
                entries.push(new_want_have_entry(cid, set_send_dont_have));
                state_entry.insert(WantReqState::SentWantHave);
            }
        }

        self.synced_revision = wantlist.revision;
        self.force_update = false;

        ProtoWantlist {
            entries,
            full: false,
        }
    }
}

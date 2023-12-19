use std::collections::hash_map;
use std::sync::{Arc, Mutex};

use cid::CidGeneric;
use fnv::{FnvHashMap, FnvHashSet};

use crate::message::{new_cancel_entry, new_want_block_entry, new_want_have_entry};
use crate::proto::message::mod_Message::mod_Wantlist::{Entry, WantType};

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

pub(crate) struct WantlistState<const S: usize> {
    wantlist: Arc<Mutex<Wantlist<S>>>,
    cids: FnvHashMap<CidGeneric<S>, WantType>,
    haves: FnvHashMap<CidGeneric<S>, bool>,
    force_update: bool,
    synced_revision: u64,
}

impl<const S: usize> WantlistState<S> {
    pub(crate) fn new(wantlist: Arc<Mutex<Wantlist<S>>>) -> Self {
        WantlistState {
            wantlist,
            cids: FnvHashMap::default(),
            haves: FnvHashMap::default(),
            force_update: true,
            synced_revision: 0,
        }
    }

    fn is_updated_locked(&self, wantlist: &Wantlist<S>) -> bool {
        !self.force_update && self.synced_revision == wantlist.revision
    }

    pub(crate) fn is_updated(&self) -> bool {
        let wantlist = self.wantlist.lock().unwrap();
        self.is_updated_locked(&*wantlist)
    }

    pub(crate) fn got_have(&mut self, cid: &CidGeneric<S>) {
        if self.cids.contains_key(cid) {
            self.haves.insert(cid.to_owned(), true);
            self.force_update = true;
        }
    }

    pub(crate) fn got_dont_have(&mut self, cid: &CidGeneric<S>) {
        if self.cids.contains_key(cid) {
            self.haves.insert(cid.to_owned(), false);
        }
    }

    pub(crate) fn got_block(&mut self, cid: &CidGeneric<S>) {
        self.cids.remove(cid);
        self.haves.remove(cid);
    }

    pub(crate) fn generate_update_entries(&mut self, set_send_dont_have: bool) -> Vec<Entry> {
        let wantlist = self.wantlist.lock().unwrap();

        if self.is_updated_locked(&*wantlist) {
            return Vec::new();
        }

        let mut entries = Vec::new();
        let mut cancelled = Vec::new();

        // Update existing entries
        for (cid, want_type) in self.cids.iter_mut() {
            match (wantlist.cids.contains(cid), *want_type, self.haves.get(cid)) {
                // If CID is not in wantlist, cancel it
                (false, _, _) => {
                    cancelled.push(cid.to_owned());
                    self.haves.remove(cid);
                    entries.push(new_cancel_entry(cid));
                }

                // If CID is in the wantlist AND we requested Have AND we got positive
                // answer, then request for Block.
                (true, WantType::Have, Some(true)) => {
                    *want_type = WantType::Block;
                    self.haves.remove(cid);
                    entries.push(new_want_block_entry(cid, set_send_dont_have));
                }

                // Peer answered with DontHave
                (true, WantType::Have, Some(false)) => {}

                // No answer yet
                (true, WantType::Have, None) => {}

                // Block was requested
                (true, WantType::Block, _) => {}
            }
        }

        // Remove canceled
        for cid in cancelled {
            self.cids.remove(&cid);
        }

        // Add new entries
        for cid in &wantlist.cids {
            if let hash_map::Entry::Vacant(cids_entry) = self.cids.entry(cid.to_owned()) {
                cids_entry.insert(WantType::Have);
                entries.push(new_want_have_entry(cid, set_send_dont_have));
            }
        }

        self.synced_revision = wantlist.revision;
        self.force_update = false;

        entries
    }
}

use super::message::*;
#[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
use flume::{Receiver, Sender};
#[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
use tracing::{instrument, trace};

/// A thread which receives and drops expensive mixer state (tracks and track
/// handles) so that a slow drop never costs the mixer its deadline.
///
/// On the host target (Emscripten inside a single-threaded JavaScript isolate)
/// there is no second thread to drop on, so `dispose` drops inline. The
/// deadline the thread protects natively belongs to playback; a receive-only
/// driver has no tracks to drop.
#[derive(Debug, Clone)]
pub struct DisposalThread(
    #[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
    Sender<DisposalMessage>,
);

impl Default for DisposalThread {
    fn default() -> Self {
        Self::run()
    }
}

#[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
impl DisposalThread {
    #[must_use]
    pub fn run() -> Self {
        let (mix_tx, mix_rx) = flume::unbounded();
        std::thread::spawn(move || {
            trace!("Disposal thread started.");
            runner(mix_rx);
            trace!("Disposal thread finished.");
        });

        Self(mix_tx)
    }

    pub(super) fn dispose(&self, message: DisposalMessage) {
        drop(self.0.send(message));
    }
}

#[cfg(all(target_os = "emscripten", not(target_feature = "atomics")))]
impl DisposalThread {
    #[must_use]
    pub fn run() -> Self {
        Self()
    }

    #[expect(
        clippy::unused_self,
        reason = "same signature as the native method, which sends to its thread"
    )]
    pub(super) fn dispose(&self, message: DisposalMessage) {
        drop(message);
    }
}

/// The mixer's disposal thread is also synchronous, due to tracks,
/// inputs, etc. being based on synchronous I/O.
///
/// The mixer uses this to offload heavy and expensive drop operations
/// to prevent deadline misses.
#[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
#[instrument(skip(mix_rx))]
#[expect(
    clippy::needless_pass_by_value,
    reason = "spawned on background thread, must take by value"
)]
fn runner(mix_rx: Receiver<DisposalMessage>) {
    while mix_rx.recv().is_ok() {}
}

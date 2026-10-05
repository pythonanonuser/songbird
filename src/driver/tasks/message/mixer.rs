#![allow(missing_docs)]

#[cfg(feature = "receive")]
use super::UdpRxMessage;
use super::{Interconnect, TrackContext, WsMessage};

use crate::{
    driver::{crypto::Cipher, Bitrate, Config, CryptoState},
    input::{AudioStreamError, Compose, Parsed},
};
use flume::Sender;
#[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
use std::net::UdpSocket;
use std::sync::{atomic::AtomicU16, Arc, RwLock};
use symphonia_core::{errors::Error as SymphoniaError, formats::SeekedTo};

pub struct MixerConnection {
    pub cipher: Cipher,
    pub crypto_state: CryptoState,
    pub dave_session: Arc<RwLock<Option<davey::DaveSession>>>,
    pub dave_protocol_version: Arc<AtomicU16>,
    #[cfg(feature = "receive")]
    pub udp_rx: Sender<UdpRxMessage>,
    #[cfg(not(all(target_os = "emscripten", not(target_feature = "atomics"))))]
    pub udp_tx: UdpSocket,
    /// On the host target (Emscripten inside a single-threaded JavaScript
    /// isolate) the socket is link-backed and has no file descriptor to clone,
    /// so the mixer shares the one `tokio::net::UdpSocket` with the receive
    /// task and sends with `try_send`, which never blocks.
    #[cfg(all(target_os = "emscripten", not(target_feature = "atomics")))]
    pub udp_tx: Arc<tokio::net::UdpSocket>,
}

pub enum MixerMessage {
    AddTrack(Box<TrackContext>),
    SetTrack(Option<Box<TrackContext>>),

    SetBitrate(Bitrate),
    SetConfig(Config),
    SetMute(bool),

    SetConn(MixerConnection, u32),
    Ws(Option<Sender<WsMessage>>),
    DropConn,

    ReplaceInterconnect(Interconnect),
    RebuildEncoder,

    Poison,
}

impl MixerMessage {
    #[must_use]
    pub fn is_mixer_maybe_live(&self) -> bool {
        matches!(
            self,
            Self::AddTrack(_) | Self::SetTrack(Some(_)) | Self::SetConn(..)
        )
    }
}

pub enum MixerInputResultMessage {
    CreateErr(Arc<AudioStreamError>),
    ParseErr(Arc<SymphoniaError>),
    Seek(
        Parsed,
        Option<Box<dyn Compose>>,
        Result<SeekedTo, Arc<SymphoniaError>>,
    ),
    Built(Parsed, Option<Box<dyn Compose>>),
}

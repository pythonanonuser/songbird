use crate::{
    error::JsonError,
    model::{deserialize_binary_event, Event},
};

use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryStreamExt};
use serenity_voice_model::{serialize_binary_event, BinaryError};
#[cfg(not(target_os = "emscripten"))]
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
#[cfg(feature = "tungstenite")]
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::CloseFrame};
#[cfg(all(feature = "tungstenite", not(target_os = "emscripten")))]
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig as Config, Message},
    MaybeTlsStream, WebSocketStream,
};
#[cfg(feature = "tws")]
use tokio_websockets::{CloseCode, Error as TwsError};
#[cfg(all(feature = "tws", not(target_os = "emscripten")))]
use tokio_websockets::{Limits, MaybeTlsStream, Message, WebSocketStream};
use tracing::{debug, instrument};
use url::Url;
#[cfg(target_os = "emscripten")]
use wsio::Message;

#[cfg(not(target_os = "emscripten"))]
pub struct WsStream(WebSocketStream<MaybeTlsStream<TcpStream>>);
#[cfg(target_os = "emscripten")]
pub struct WsStream(Box<dyn wsio::Transport>);

impl WsStream {
    #[instrument]
    pub(crate) async fn connect(url: Url) -> Result<Self> {
        #[cfg(all(feature = "tungstenite", not(target_os = "emscripten")))]
        let (stream, _) = tokio_tungstenite::connect_async_with_config::<Url>(
            url,
            Some(
                Config::default()
                    .max_message_size(None)
                    .max_frame_size(None),
            ),
            true,
        )
        .await?;
        #[cfg(all(feature = "tws", not(target_os = "emscripten")))]
        let (stream, _) = tokio_websockets::ClientBuilder::new()
            .limits(Limits::unlimited())
            .uri(url.as_str())
            .unwrap() // Any valid URL is a valid URI.
            .connect()
            .await?;
        #[cfg(target_os = "emscripten")]
        let stream = wsio::connect(
            http::Request::builder()
                .uri(url.as_str())
                .body(())
                .expect("Any valid URL is a valid URI."),
        )
        .await?;

        Ok(Self(stream))
    }

    pub(crate) async fn recv_event(&mut self) -> Result<Option<Event>> {
        const TIMEOUT: Duration = Duration::from_millis(500);

        let ws_message = match timeout(TIMEOUT, self.0.next()).await {
            Ok(Some(Ok(v))) => Some(v),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => {
                #[cfg(target_os = "emscripten")]
                return Err(Error::WsClosed(None));
                #[cfg(not(target_os = "emscripten"))]
                None
            },
            Err(_) => None,
        };

        convert_ws_message(ws_message)
    }

    pub(crate) async fn recv_event_no_timeout(&mut self) -> Result<Option<Event>> {
        let message = self.0.try_next().await?;
        #[cfg(target_os = "emscripten")]
        if message.is_none() {
            return Err(Error::WsClosed(None));
        }
        convert_ws_message(message)
    }

    pub(crate) async fn send_json(&mut self, value: &Event) -> Result<()> {
        let res = crate::json::to_string(value);
        let res = res.map(Message::text);
        Ok(res.map_err(Error::from).map(|m| self.0.send(m))?.await?)
    }

    pub(crate) async fn send_binary(&mut self, value: &Event) -> Result<()> {
        let res = serialize_binary_event(value);
        let res = res.map(Message::binary);

        Ok(res.map_err(Error::from).map(|m| self.0.send(m))?.await?)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Json(JsonError),

    #[cfg(target_os = "emscripten")]
    Io(std::io::Error),

    /// The discord voice gateway does not support or offer zlib compression.
    /// As a result, only text messages are expected.
    UnexpectedBinaryMessage(Bytes),

    #[cfg(feature = "tungstenite")]
    Ws(Box<TungsteniteError>),
    #[cfg(feature = "tws")]
    Ws(TwsError),

    #[cfg(feature = "tungstenite")]
    WsClosed(Option<CloseFrame>),
    #[cfg(feature = "tws")]
    WsClosed(Option<CloseCode>),

    Binary(BinaryError),
}

impl From<JsonError> for Error {
    fn from(e: JsonError) -> Error {
        Error::Json(e)
    }
}

#[cfg(target_os = "emscripten")]
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

#[cfg(feature = "tungstenite")]
impl From<TungsteniteError> for Error {
    fn from(e: TungsteniteError) -> Error {
        Error::Ws(Box::new(e))
    }
}

#[cfg(feature = "tws")]
impl From<TwsError> for Error {
    fn from(e: TwsError) -> Self {
        Error::Ws(e)
    }
}

impl From<BinaryError> for Error {
    fn from(value: BinaryError) -> Self {
        Error::Binary(value)
    }
}

#[inline]
pub(crate) fn convert_ws_message(message: Option<Message>) -> Result<Option<Event>> {
    #[cfg(target_os = "emscripten")]
    match message {
        Some(Message::Text(payload)) => {
            return Ok(serde_json::from_str(&payload)
                .map_err(|e| {
                    debug!("Unexpected JSON: {e}. Payload: {payload}");
                    e
                })
                .ok());
        },
        Some(Message::Binary(bytes)) => {
            return Ok(deserialize_binary_event(&bytes)
                .map_err(|e| {
                    debug!("Unexpected binary: {e}");
                    e
                })
                .ok());
        },
        Some(Message::Close { code, reason }) => {
            #[cfg(feature = "tungstenite")]
            return Err(Error::WsClosed((code != 1005).then(|| CloseFrame {
                code: code.into(),
                reason: reason.into(),
            })));
            #[cfg(feature = "tws")]
            return Err(Error::WsClosed(CloseCode::try_from(code).ok()));
        },
        // workerd handles WebSocket control frames.
        _ => return Ok(None),
    };

    #[cfg(all(feature = "tungstenite", not(target_os = "emscripten")))]
    match message {
        Some(Message::Text(ref payload)) => {
            return Ok(serde_json::from_str(payload)
                .map_err(|e| {
                    debug!("Unexpected JSON: {e}. Payload: {payload}");
                    e
                })
                .ok())
        },
        Some(Message::Binary(bytes)) => {
            return Ok(deserialize_binary_event(&bytes)
                .map_err(|e| {
                    debug!("Unexpected binary: {e}");
                    e
                })
                .ok());
        },
        Some(Message::Close(Some(frame))) => {
            return Err(Error::WsClosed(Some(frame)));
        },
        // Ping/Pong message behaviour is internally handled by tungstenite.
        _ => return Ok(None),
    };

    #[cfg(all(feature = "tws", not(target_os = "emscripten")))]
    match message {
        Some(ref message) if message.is_text() => {
            return if let Some(text) = message.as_text() {
                Ok(serde_json::from_str(text)
                    .map_err(|e| {
                        debug!("Unexpected JSON: {e}. Payload: {text}");
                        e
                    })
                    .ok())
            } else {
                Ok(None)
            };
        },
        Some(message) if message.is_binary() => {
            return Ok(deserialize_binary_event(&message.into_payload())
                .map_err(|e| {
                    debug!("Unexpected binary: {e}");
                    e
                })
                .ok());
        },
        Some(message) if message.is_close() => {
            return Err(Error::WsClosed(message.as_close().map(|(c, _)| c)));
        },
        // ping/pong; will also be internally handled by tokio-websockets.
        _ => return Ok(None),
    };
}

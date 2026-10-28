//! Everything Kalshi-specific: auth, REST discovery, and the websocket
//! protocol. The engine, book and writer don't know which exchange they serve.

pub mod auth;
pub mod protocol;
pub mod rest;
pub mod socket;

pub const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
pub const REST_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// The path that gets signed for the websocket handshake.
pub const WS_PATH: &str = "/trade-api/ws/v2";

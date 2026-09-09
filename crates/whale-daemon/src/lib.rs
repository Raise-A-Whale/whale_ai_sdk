//! whale-daemon: IPC daemon server and Reverse RPC host bridge for Whale AI SDK.

mod context_bridge;
pub mod server;
pub mod transport;

pub use server::{DaemonServer, HostToolBridge};
pub use transport::{AnyTransportWriter, OutgoingTransport, StdioWriter, UnixStreamWriter};

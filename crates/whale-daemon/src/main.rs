//! whale-daemon CLI binary entrypoint.

use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use tokio_stream::wrappers::LinesStream;

use whale_daemon::{DaemonServer, StdioWriter, UnixStreamWriter};

/// Whale AI SDK Daemon process managing local agents and IPC transport.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Transport listen mode: "stdio" or "uds://<path>"
    #[arg(short, long, default_value = "stdio")]
    listen: String,

    /// Log level filter (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // When running stdio, log only to stderr so stdout is purely for JSON-RPC framing!
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    info!("Starting whale-daemon (listen={})", cli.listen);

    let server = Arc::new(DaemonServer::default_server());

    if cli.listen == "stdio" {
        let stdin = tokio::io::stdin();
        let stdout = Arc::new(Mutex::new(tokio::io::stdout()));

        let reader = BufReader::new(stdin);
        let lines_stream = LinesStream::new(reader.lines());
        let transport = StdioWriter::new(stdout);

        server.run(lines_stream, transport).await?;
    } else if let Some(uds_path) = cli.listen.strip_prefix("uds://") {
        let socket_path = PathBuf::from(uds_path);
        if socket_path.exists() {
            let _ = std::fs::remove_file(&socket_path);
        }

        let listener = UnixListener::bind(&socket_path)?;
        info!("Listening on Unix domain socket: {:?}", socket_path);

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let server_clone = Arc::clone(&server);
                    tokio::spawn(async move {
                        let (read_half, write_half) = stream.into_split();
                        let reader = BufReader::new(read_half);
                        let lines_stream = LinesStream::new(reader.lines());
                        let transport = UnixStreamWriter::new(write_half);

                        if let Err(e) = server_clone.run(lines_stream, transport).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Listener accept error: {}", e);
                    break;
                }
            }
        }
    } else {
        error!("Unsupported listen mode: {}", cli.listen);
        std::process::exit(1);
    }

    Ok(())
}

use futures::TryStreamExt;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use whale_adapters::{AnthropicAdapter, SamplingOptions};
use whale_core::http_provider::HttpModelProvider;
use whale_core::model::{ModelError, ModelEvent, ModelProvider, ModelRequest};
use whale_core::CancellationToken;
use whale_protocol::contexts::{ModelContext, RunContextInfo};
use whale_protocol::CanonicalItem;

const API_KEY: &str = "fake-anthropic-redirect-test-key";
const TIMEOUT: Duration = Duration::from_secs(2);
const SSE: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n",
    "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":2}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);

fn request() -> ModelRequest {
    ModelRequest {
        context: RunContextInfo {
            agent_name: None,
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            deadline_unix_ms: None,
        },
        step_index: 0,
        step_id: "step".into(),
        model_context: ModelContext {
            system_prompt: None,
            items: vec![CanonicalItem::user_text("hello")],
        },
        tools: vec![],
        options: SamplingOptions::new("claude-test"),
    }
}

fn adapter(listener: &TcpListener) -> Arc<AnthropicAdapter> {
    Arc::new(AnthropicAdapter::with_base_url(
        API_KEY,
        format!("http://{}", listener.local_addr().unwrap()),
    ))
}

fn response(status: &str, extra_headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
        body.len()
    )
}

async fn respond(listener: &TcpListener, response: &str) -> String {
    tokio::time::timeout(TIMEOUT, async {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        let mut headers = String::new();
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse::<usize>().unwrap();
                }
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; content_length];
        socket.read_exact(&mut body).await.unwrap();
        socket
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
        headers
    })
    .await
    .expect("mock HTTP request timed out")
}

async fn assert_success(provider: &HttpModelProvider) {
    let events: Vec<ModelEvent> = tokio::time::timeout(TIMEOUT, async {
        provider
            .stream(request(), CancellationToken::new())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    })
    .await
    .expect("model response timed out");
    assert!(
        matches!(events.as_slice(), [ModelEvent::StepFinished { usage }]
        if usage.input_tokens == 1 && usage.output_tokens == 2)
    );
}

#[tokio::test]
async fn default_client_does_not_follow_redirects_or_forward_api_keys() {
    for status in [307, 308, 301, 302, 303] {
        let destination = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        destination.set_nonblocking(true).unwrap();
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = HttpModelProvider::new(adapter(&origin));
        let redirect = response(
            &format!("{status} Redirect"),
            &format!(
                "Location: http://{}/capture\r\n",
                destination.local_addr().unwrap()
            ),
            "",
        );
        let peer = tokio::spawn(async move { respond(&origin, &redirect).await });
        let result = tokio::time::timeout(
            TIMEOUT,
            provider.stream(request(), CancellationToken::new()),
        )
        .await;
        let headers = peer.await.unwrap();
        assert!(headers.contains(&format!("x-api-key: {API_KEY}\r\n")));
        assert!(
            matches!(destination.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "the default client followed HTTP {status} to another origin"
        );
        assert!(
            matches!(result.unwrap(), Err(ModelError::Transport(message))
            if message.contains(&format!("API error {status}")))
        );
    }
}

#[tokio::test]
async fn default_client_still_streams_successful_responses() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = HttpModelProvider::new(adapter(&origin));
    let peer = tokio::spawn(async move {
        respond(
            &origin,
            &response("200 OK", "Content-Type: text/event-stream\r\n", SSE),
        )
        .await
    });
    assert_success(&provider).await;
    let headers = peer.await.unwrap();
    assert!(headers.starts_with("POST /messages HTTP/1.1\r\n"));
    assert!(headers.contains(&format!("x-api-key: {API_KEY}\r\n")));
}

#[tokio::test]
async fn custom_client_retains_its_explicit_redirect_policy() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(1))
        .user_agent("whale-custom-client-test")
        .build()
        .unwrap();
    let provider = HttpModelProvider::with_client(adapter(&origin), client);
    let peer = tokio::spawn(async move {
        respond(
            &origin,
            &response("307 Temporary Redirect", "Location: /final\r\n", ""),
        )
        .await;
        respond(
            &origin,
            &response("200 OK", "Content-Type: text/event-stream\r\n", SSE),
        )
        .await
    });
    assert_success(&provider).await;
    let headers = peer.await.unwrap();
    assert!(headers.starts_with("POST /final HTTP/1.1\r\n"));
    assert!(headers.contains("user-agent: whale-custom-client-test\r\n"));
    assert!(headers.contains(&format!("x-api-key: {API_KEY}\r\n")));
}

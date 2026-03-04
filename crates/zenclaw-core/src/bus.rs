//! Event Bus — async pub/sub message passing between components.

use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::message::{InboundMessage, OutboundMessage};

/// Event types flowing through the bus.
#[derive(Debug, Clone)]
pub enum BusEvent {
    /// Incoming message from a channel.
    Inbound(InboundMessage),
    /// Outgoing message to a channel.
    Outbound(OutboundMessage),
    /// System event (lifecycle, tool, error).
    System(SystemEvent),
}

/// System event for monitoring.
#[derive(Debug, Clone)]
pub struct SystemEvent {
    pub run_id: String,
    pub event_type: String,
    pub data: serde_json::Value,
}

impl SystemEvent {
    /// Format a system event into a human-readable status string for the CLI.
    pub fn format_status(&self) -> Option<String> {
        match self.event_type.as_str() {
            "agent_think" => {
                let it = self.data["iteration"].as_u64().unwrap_or(0);
                if it == 1 {
                    Some("🧠 Analyzing your question...".to_string())
                } else {
                    Some(format!("🔄 Processing results, reasoning step {}...", it))
                }
            }

            "tool_use" => {
                let tool = self.data["tool"].as_str().unwrap_or("tool");

                // Extract the most relevant argument from args JSON
                let args: serde_json::Value = self.data["args"]
                    .as_str()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(serde_json::Value::Null);

                let context = extract_tool_context(&args);

                let emoji_action = match tool {
                    "web_search"            => format!("🔍 Searching the web{}", context),
                    "web_fetch"             => format!("🌐 Fetching page{}", context),
                    "web_scrape"            => format!("📄 Reading page content{}", context),
                    "read_file"             => format!("📂 Reading file{}", context),
                    "write_file"            => format!("✏️  Writing file{}", context),
                    "edit_file"             => format!("🔧 Editing file{}", context),
                    "list_dir"              => format!("📁 Listing directory{}", context),
                    "shell" | "exec"        => format!("⚡ Running command{}", context),
                    "process"               => format!("🔄 Managing process{}", context),
                    "sub_agent"             => format!("🤖 Spawning sub-agent{}", context),
                    "system_info"           => "💻 Checking system info...".to_string(),
                    "history"               => "🕒 Reading conversation history...".to_string(),
                    "env"                   => "🌍 Checking environment...".to_string(),
                    "health"                => "❤️  Running health check...".to_string(),
                    "cron"                  => format!("⏱️  Scheduling task{}", context),
                    _                       => format!("🛠️  Running '{}'{}", tool, context),
                };

                Some(emoji_action)
            }

            "tool_result" => {
                let tool = self.data["tool"].as_str().unwrap_or("");
                let len  = self.data["result_len"].as_u64().unwrap_or(0);

                let msg = match tool {
                    "web_search"  => format!("✅ Got search results ({} bytes) — thinking...", len),
                    "web_fetch"   => format!("✅ Page fetched ({} bytes) — analyzing...", len),
                    "web_scrape"  => format!("✅ Content extracted ({} bytes) — analyzing...", len),
                    "read_file"   => format!("✅ File read ({} bytes) — processing...", len),
                    "shell" | "exec" => format!("✅ Command finished ({} bytes output) — evaluating...", len),
                    _             => "✅ Done — reasoning about results...".to_string(),
                };
                Some(msg)
            }

            "memory_truncate" => {
                let status = self.data["status"].as_str().unwrap_or("📚 Summarizing older conversation into Memory Card...");
                Some(status.to_string())
            }

            "tool_timeout" => {
                let tool = self.data["tool"].as_str().unwrap_or("tool");
                Some(format!("⚠️  '{}' timed out — trying a different approach...", tool))
            }

            "llm_retry" => {
                let attempt = self.data["attempt"].as_u64().unwrap_or(1);
                let is_rate_limit = self.data["is_rate_limit"].as_bool().unwrap_or(false);
                let wait_ms = self.data["wait_ms"].as_u64().unwrap_or(2000);
                
                if is_rate_limit {
                    Some(format!("⏳ Rate limit hit (API quota/limits). Waiting {}s before retry (attempt {})...", wait_ms / 1000, attempt))
                } else {
                    Some(format!("🔁 Connection hiccup, retrying in {}s... (attempt {})", wait_ms / 1000, attempt))
                }
            }

            "rag_inject" => {
                Some("📚 RAG context found — injecting relevant knowledge...".to_string())
            }

            "rag_search" => {
                Some("🔎 Searching knowledge base for relevant context...".to_string())
            }

            "json_retry" => {
                Some("🔄 Response wasn't valid JSON — retrying with correction...".to_string())
            }

            "agent_reasoning" => {
                let reasoning = self.data["reasoning"].as_str().unwrap_or("");
                let preview = if reasoning.len() > 100 { &reasoning[..100] } else { reasoning };
                Some(format!("💭 Thinking: {}...", preview))
            }

            "answer_expansion" => {
                let len = self.data["original_len"].as_u64().unwrap_or(0);
                Some(format!("📝 Answer too brief ({} chars) — requesting detailed elaboration...", len))
            }

            "answer_audit" => {
                Some("🔍 Auditing answer quality before delivering...".to_string())
            }

            "audit_result" => {
                let score = self.data["score"].as_u64().unwrap_or(0);
                let pass = self.data["pass"].as_bool().unwrap_or(true);
                if pass {
                    Some(format!("✅ Quality audit passed ({}/10)", score))
                } else {
                    Some(format!("⚠️  Quality audit: {}/10 — needs improvement", score))
                }
            }

            "answer_refine" => {
                let status = self.data["status"].as_str().unwrap_or("Refining...");
                Some(format!("🔧 {}", status))
            }

            "refine_complete" => {
                Some("✅ Refinement finished".to_string())
            }

            _ => None,
        }
    }
}

/// Extract the most relevant context string from tool arguments.
fn extract_tool_context(args: &serde_json::Value) -> String {
    // Priority: query > url > path > command > (nothing)
    let raw = if let Some(q) = args["query"].as_str() {
        q
    } else if let Some(u) = args["url"].as_str() {
        u
    } else if let Some(p) = args["path"].as_str() {
        p
    } else if let Some(c) = args["command"].as_str() {
        c
    } else if let Some(c) = args["cmd"].as_str() {
        c
    } else {
        return String::new();
    };

    // Truncate long context for display
    let display = if raw.len() > 60 {
        format!("{}...", &raw[..60])
    } else {
        raw.to_string()
    };
    format!(": \"{}\"", display)
}



/// The event bus — central nervous system of ZenClaw.
///
/// Components publish events, other components subscribe to them.
/// Uses tokio channels for async, non-blocking communication.
pub struct EventBus {
    inbound_tx: mpsc::Sender<InboundMessage>,
    inbound_rx: Arc<Mutex<mpsc::Receiver<InboundMessage>>>,
    outbound_tx: broadcast::Sender<OutboundMessage>,
    system_tx: broadcast::Sender<SystemEvent>,
}

impl EventBus {
    pub fn new(buffer_size: usize) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(buffer_size);
        let (outbound_tx, _) = broadcast::channel(buffer_size);
        let (system_tx, _) = broadcast::channel(buffer_size);

        Self {
            inbound_tx,
            inbound_rx: Arc::new(Mutex::new(inbound_rx)),
            outbound_tx,
            system_tx,
        }
    }

    /// Publish an inbound message (from channel → agent).
    pub async fn publish_inbound(&self, msg: InboundMessage) {
        if let Err(e) = self.inbound_tx.send(msg).await {
            tracing::error!("Failed to publish inbound: {}", e);
        }
    }

    /// Receive the next inbound message (agent consumes).
    pub async fn recv_inbound(&self) -> Option<InboundMessage> {
        let mut rx = self.inbound_rx.lock().await;
        rx.recv().await
    }

    /// Publish an outbound message (agent → channel).
    pub fn publish_outbound(&self, msg: OutboundMessage) {
        let _ = self.outbound_tx.send(msg);
    }

    /// Subscribe to outbound messages (channels consume).
    pub fn subscribe_outbound(&self) -> broadcast::Receiver<OutboundMessage> {
        self.outbound_tx.subscribe()
    }

    /// Publish a system event (monitoring).
    pub fn publish_system(&self, event: SystemEvent) {
        let _ = self.system_tx.send(event);
    }

    /// Subscribe to system events.
    pub fn subscribe_system(&self) -> broadcast::Receiver<SystemEvent> {
        self.system_tx.subscribe()
    }

    /// Get a clone of the inbound sender (for channels to use).
    pub fn inbound_sender(&self) -> mpsc::Sender<InboundMessage> {
        self.inbound_tx.clone()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(256)
    }
}

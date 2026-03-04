//! Error types for ZenClaw.

use serde::Serialize;
use thiserror::Error;

/// Core error type for all ZenClaw operations.
#[derive(Error, Debug)]
pub enum ZenClawError {
    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Tool execution error: {tool} — {message}")]
    ToolExecution { tool: String, message: String },

    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Memory error: {0}")]
    Memory(String),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Max iterations reached ({0})")]
    MaxIterations(usize),

    #[error("{0}")]
    Other(String),
}

impl ZenClawError {
    /// Machine-readable error code for consistent API error envelopes.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Provider(_) => "PROVIDER_ERROR",
            Self::ToolExecution { .. } => "TOOL_EXECUTION_ERROR",
            Self::ToolNotFound(_) => "TOOL_NOT_FOUND",
            Self::Config(_) => "CONFIG_ERROR",
            Self::Memory(_) => "MEMORY_ERROR",
            Self::Network(_) => "NETWORK_ERROR",
            Self::Json(_) => "JSON_ERROR",
            Self::Io(_) => "IO_ERROR",
            Self::MaxIterations(_) => "MAX_ITERATIONS",
            Self::Other(_) => "INTERNAL_ERROR",
        }
    }
}

/// Standard API error envelope for consistent client-side error handling.
#[derive(Debug, Serialize)]
pub struct ApiErrorEnvelope {
    pub error: bool,
    pub code: String,
    pub message: String,
}

impl ApiErrorEnvelope {
    /// Create from a ZenClawError.
    pub fn from_error(e: &ZenClawError) -> Self {
        Self {
            error: true,
            code: e.error_code().to_string(),
            message: e.to_string(),
        }
    }

    /// Create from a raw code and message.
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            error: true,
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ZenClawError>;

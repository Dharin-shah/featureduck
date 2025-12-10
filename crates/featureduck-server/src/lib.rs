//! FeatureDuck Server Library
//!
//! This exposes the server modules for integration testing.
//!
//! ## P0 Security Features
//!
//! - **Authentication**: API key authentication via `auth` module
//! - **Rate Limiting**: Request rate limiting via `rate_limit` module
//!
//! ## Usage
//!
//! ```rust,ignore
//! use featureduck_server::{auth::AuthConfig, rate_limit::RateLimitState};
//!
//! // Load config from environment
//! let auth_config = AuthConfig::from_env();
//! let rate_limit_state = RateLimitState::from_env();
//! ```

pub mod api;
pub mod auth;
pub mod error;
pub mod health;
pub mod metrics;
pub mod rate_limit;
pub mod request_id;
pub mod shutdown;
pub mod state;

//! Time Window Definitions for Feature Aggregation
//!
//! This module provides time window specifications for feature views,
//! enabling efficient incremental computation of time-windowed features.
//!
//! ## Window Types
//!
//! - **Tumbling**: Non-overlapping fixed windows (e.g., hourly buckets)
//! - **Sliding**: Overlapping windows (e.g., last 7 days, updated every hour)
//! - **Session**: Gap-based windows (e.g., 30-min inactivity timeout)
//!
//! ## Example
//!
//! ```rust
//! use featureduck_core::time_window::{TimeWindow, WindowType};
//! use std::time::Duration;
//!
//! // Last 7 days tumbling window (recomputed daily)
//! let window = TimeWindow::tumbling(Duration::from_secs(7 * 86400));
//!
//! // Last 24 hours sliding window (updated every hour)
//! let window = TimeWindow::sliding(
//!     Duration::from_secs(24 * 3600),
//!     Duration::from_secs(3600)
//! );
//! ```

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Time window type for aggregations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    /// Non-overlapping fixed-size windows
    ///
    /// Example: Hourly windows where each event belongs to exactly one window
    Tumbling,

    /// Overlapping windows with slide interval
    ///
    /// Example: 24-hour windows sliding every hour
    /// An event at 3:30 PM belongs to windows starting at 2PM, 3PM, etc.
    Sliding,

    /// Gap-based session windows
    ///
    /// Example: User session ends after 30 minutes of inactivity
    Session,

    /// Growing window from a fixed start time
    ///
    /// Example: "All time" aggregations since account creation
    Cumulative,
}

/// Time window specification for feature aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    /// Window type
    pub window_type: WindowType,

    /// Window duration (total size of window)
    pub duration: Duration,

    /// Slide interval for sliding windows (how often the window advances)
    /// For tumbling windows, this equals duration
    /// For session windows, this is the gap timeout
    pub slide: Duration,

    /// Human-readable name (e.g., "7d", "24h", "30m")
    pub name: String,
}

impl TimeWindow {
    /// Create a tumbling window with the given duration
    ///
    /// Tumbling windows are non-overlapping and partition time into
    /// fixed-size buckets.
    ///
    /// # Example
    /// ```rust
    /// use featureduck_core::time_window::TimeWindow;
    /// use std::time::Duration;
    ///
    /// let hourly = TimeWindow::tumbling(Duration::from_secs(3600));
    /// let daily = TimeWindow::tumbling(Duration::from_secs(86400));
    /// ```
    pub fn tumbling(duration: Duration) -> Self {
        Self {
            window_type: WindowType::Tumbling,
            duration,
            slide: duration, // Tumbling: slide == duration
            name: Self::duration_to_name(duration),
        }
    }

    /// Create a sliding window with the given duration and slide interval
    ///
    /// Sliding windows overlap and produce more frequent updates.
    ///
    /// # Example
    /// ```rust
    /// use featureduck_core::time_window::TimeWindow;
    /// use std::time::Duration;
    ///
    /// // 24-hour window, updated every hour
    /// let window = TimeWindow::sliding(
    ///     Duration::from_secs(24 * 3600),
    ///     Duration::from_secs(3600)
    /// );
    /// ```
    pub fn sliding(duration: Duration, slide: Duration) -> Self {
        Self {
            window_type: WindowType::Sliding,
            duration,
            slide,
            name: format!(
                "{}_slide_{}",
                Self::duration_to_name(duration),
                Self::duration_to_name(slide)
            ),
        }
    }

    /// Create a session window with the given gap timeout
    ///
    /// Session windows group events that occur within the gap timeout
    /// of each other. A new session starts after the gap.
    ///
    /// # Example
    /// ```rust
    /// use featureduck_core::time_window::TimeWindow;
    /// use std::time::Duration;
    ///
    /// // 30-minute session timeout
    /// let session = TimeWindow::session(Duration::from_secs(30 * 60));
    /// ```
    pub fn session(gap_timeout: Duration) -> Self {
        Self {
            window_type: WindowType::Session,
            duration: gap_timeout, // Session duration is determined by events
            slide: gap_timeout,    // Gap timeout
            name: format!("session_{}", Self::duration_to_name(gap_timeout)),
        }
    }

    /// Create a cumulative (all-time) window
    ///
    /// Cumulative windows grow from a fixed start time and include
    /// all events since then.
    pub fn cumulative() -> Self {
        Self {
            window_type: WindowType::Cumulative,
            duration: Duration::MAX,
            slide: Duration::MAX,
            name: "all_time".to_string(),
        }
    }

    /// Convert duration to human-readable name
    fn duration_to_name(duration: Duration) -> String {
        let secs = duration.as_secs();
        if secs.is_multiple_of(604800) && secs >= 604800 {
            format!("{}w", secs / 604800)
        } else if secs.is_multiple_of(86400) && secs >= 86400 {
            format!("{}d", secs / 86400)
        } else if secs.is_multiple_of(3600) && secs >= 3600 {
            format!("{}h", secs / 3600)
        } else if secs.is_multiple_of(60) && secs >= 60 {
            format!("{}m", secs / 60)
        } else {
            format!("{}s", secs)
        }
    }

    /// Get the bucket key for a timestamp
    ///
    /// For tumbling/sliding windows, this returns the bucket index.
    /// For session windows, this is more complex (needs event history).
    pub fn bucket_key(&self, timestamp_secs: i64) -> i64 {
        match self.window_type {
            WindowType::Tumbling | WindowType::Sliding => {
                timestamp_secs / self.slide.as_secs() as i64
            }
            WindowType::Session => {
                // Session windows are determined by events, not fixed buckets
                // Return the timestamp itself as a pseudo-bucket
                timestamp_secs
            }
            WindowType::Cumulative => {
                // Cumulative windows have a single bucket
                0
            }
        }
    }

    /// Get the window start time for a bucket key
    pub fn window_start(&self, bucket_key: i64) -> i64 {
        match self.window_type {
            WindowType::Tumbling | WindowType::Sliding => bucket_key * self.slide.as_secs() as i64,
            WindowType::Session => bucket_key, // Session start is the first event
            WindowType::Cumulative => 0,       // From beginning of time
        }
    }

    /// Get the window end time for a bucket key
    pub fn window_end(&self, bucket_key: i64) -> i64 {
        match self.window_type {
            WindowType::Tumbling => (bucket_key + 1) * self.duration.as_secs() as i64,
            WindowType::Sliding => {
                bucket_key * self.slide.as_secs() as i64 + self.duration.as_secs() as i64
            }
            WindowType::Session => {
                // Session end is determined by the last event + gap
                bucket_key + self.slide.as_secs() as i64
            }
            WindowType::Cumulative => i64::MAX,
        }
    }

    /// Check if this window is a sliding window
    pub fn is_sliding(&self) -> bool {
        self.window_type == WindowType::Sliding
    }

    /// Check if this window is a tumbling window
    pub fn is_tumbling(&self) -> bool {
        self.window_type == WindowType::Tumbling
    }

    /// Get the number of slides per window duration
    ///
    /// For tumbling windows, this is 1.
    /// For sliding windows, this is duration / slide.
    pub fn slides_per_window(&self) -> u64 {
        if self.slide.as_secs() > 0 {
            self.duration.as_secs() / self.slide.as_secs()
        } else {
            1
        }
    }
}

/// Standard time window presets
impl TimeWindow {
    /// 1 hour tumbling window
    pub fn hourly() -> Self {
        Self::tumbling(Duration::from_secs(3600))
    }

    /// 1 day tumbling window
    pub fn daily() -> Self {
        Self::tumbling(Duration::from_secs(86400))
    }

    /// 7 day tumbling window
    pub fn weekly() -> Self {
        Self::tumbling(Duration::from_secs(7 * 86400))
    }

    /// Last 24 hours sliding window (hourly updates)
    pub fn last_24h() -> Self {
        Self::sliding(Duration::from_secs(24 * 3600), Duration::from_secs(3600))
    }

    /// Last 7 days sliding window (daily updates)
    pub fn last_7d() -> Self {
        Self::sliding(Duration::from_secs(7 * 86400), Duration::from_secs(86400))
    }

    /// Last 30 days sliding window (daily updates)
    pub fn last_30d() -> Self {
        Self::sliding(Duration::from_secs(30 * 86400), Duration::from_secs(86400))
    }
}

/// Feature definition with time window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowedFeature {
    /// Feature name (e.g., "clicks_7d")
    pub name: String,

    /// Source column to aggregate
    pub source_column: String,

    /// Aggregation function
    pub aggregation: WindowAggregation,

    /// Time window specification
    pub window: TimeWindow,

    /// Optional filter expression
    pub filter: Option<String>,
}

/// Aggregation functions for windowed features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowAggregation {
    /// COUNT(*)
    Count,
    /// COUNT(column)
    CountColumn(String),
    /// COUNT(DISTINCT column)
    CountDistinct(String),
    /// SUM(column)
    Sum(String),
    /// AVG(column)
    Avg(String),
    /// MIN(column)
    Min(String),
    /// MAX(column)
    Max(String),
    /// FIRST(column)
    First(String),
    /// LAST(column)
    Last(String),
}

impl WindowedFeature {
    /// Create a count feature
    pub fn count(name: &str, window: TimeWindow) -> Self {
        Self {
            name: name.to_string(),
            source_column: "*".to_string(),
            aggregation: WindowAggregation::Count,
            window,
            filter: None,
        }
    }

    /// Create a sum feature
    pub fn sum(name: &str, column: &str, window: TimeWindow) -> Self {
        Self {
            name: name.to_string(),
            source_column: column.to_string(),
            aggregation: WindowAggregation::Sum(column.to_string()),
            window,
            filter: None,
        }
    }

    /// Create a count distinct feature
    pub fn count_distinct(name: &str, column: &str, window: TimeWindow) -> Self {
        Self {
            name: name.to_string(),
            source_column: column.to_string(),
            aggregation: WindowAggregation::CountDistinct(column.to_string()),
            window,
            filter: None,
        }
    }

    /// Add a filter to the feature
    pub fn with_filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }

    /// Get the full feature name including window suffix
    pub fn full_name(&self) -> String {
        format!("{}_{}", self.name, self.window.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window() {
        let window = TimeWindow::tumbling(Duration::from_secs(3600));

        assert!(window.is_tumbling());
        assert!(!window.is_sliding());
        assert_eq!(window.name, "1h");
        assert_eq!(window.slides_per_window(), 1);
    }

    #[test]
    fn test_sliding_window() {
        let window = TimeWindow::sliding(Duration::from_secs(24 * 3600), Duration::from_secs(3600));

        assert!(window.is_sliding());
        assert!(!window.is_tumbling());
        // 24h = 1d, so name is "1d_slide_1h"
        assert_eq!(window.name, "1d_slide_1h");
        assert_eq!(window.slides_per_window(), 24);
    }

    #[test]
    fn test_bucket_key() {
        let window = TimeWindow::hourly();
        let ts = 1699920000i64; // Some timestamp

        let key = window.bucket_key(ts);
        let start = window.window_start(key);
        let end = window.window_end(key);

        assert!(start <= ts);
        assert!(ts < end);
        assert_eq!(end - start, 3600);
    }

    #[test]
    fn test_duration_to_name() {
        assert_eq!(TimeWindow::tumbling(Duration::from_secs(60)).name, "1m");
        assert_eq!(TimeWindow::tumbling(Duration::from_secs(3600)).name, "1h");
        assert_eq!(TimeWindow::tumbling(Duration::from_secs(86400)).name, "1d");
        assert_eq!(TimeWindow::tumbling(Duration::from_secs(604800)).name, "1w");
        assert_eq!(
            TimeWindow::tumbling(Duration::from_secs(7 * 86400)).name,
            "1w"
        );
    }

    #[test]
    fn test_preset_windows() {
        let hourly = TimeWindow::hourly();
        assert_eq!(hourly.duration.as_secs(), 3600);

        let last_7d = TimeWindow::last_7d();
        assert_eq!(last_7d.duration.as_secs(), 7 * 86400);
        assert_eq!(last_7d.slide.as_secs(), 86400);
    }

    #[test]
    fn test_windowed_feature() {
        let feature = WindowedFeature::count("clicks", TimeWindow::last_7d())
            .with_filter("event_type = 'click'");

        // 7d window with 1d slide = "1w_slide_1d"
        assert_eq!(feature.full_name(), "clicks_1w_slide_1d");
        assert!(feature.filter.is_some());
    }

    #[test]
    fn test_session_window() {
        let session = TimeWindow::session(Duration::from_secs(30 * 60));

        assert_eq!(session.window_type, WindowType::Session);
        assert_eq!(session.name, "session_30m");
    }

    #[test]
    fn test_cumulative_window() {
        let cumulative = TimeWindow::cumulative();

        assert_eq!(cumulative.window_type, WindowType::Cumulative);
        assert_eq!(cumulative.name, "all_time");
        assert_eq!(cumulative.bucket_key(12345), 0); // Always bucket 0
    }
}

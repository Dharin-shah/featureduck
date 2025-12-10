//! Output formatting utilities

use anyhow::Result;
use comfy_table::{presets::UTF8_FULL, Cell, Color, ContentArrangement, Table};
use console::style;
use serde::Serialize;

/// Output format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
}

impl OutputFormat {
    /// Parse output format from string
    /// Unlike FromStr trait, this never fails - unknown values default to Table
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            "yaml" | "yml" => Self::Yaml,
            _ => Self::Table,
        }
    }
}

/// Print data in the specified format
pub fn print_output<T: Serialize>(data: &T, format: &str) -> Result<()> {
    match OutputFormat::parse(format) {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(data)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(data)?);
        }
        OutputFormat::Table => {
            // Table format handled by specific functions
            println!("{}", serde_json::to_string_pretty(data)?);
        }
    }
    Ok(())
}

/// Create a styled table
pub fn create_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

/// Print success message
pub fn print_success(msg: &str) {
    println!("{} {}", style("✓").green().bold(), msg);
}

/// Print error message
pub fn print_error(msg: &str) {
    eprintln!("{} {}", style("✗").red().bold(), msg);
}

/// Print warning message
pub fn print_warning(msg: &str) {
    println!("{} {}", style("⚠").yellow().bold(), msg);
}

/// Print info message
pub fn print_info(msg: &str) {
    println!("{} {}", style("ℹ").blue().bold(), msg);
}

/// Format status with color
pub fn format_status(status: &str) -> Cell {
    match status.to_lowercase().as_str() {
        "success" | "completed" => Cell::new(status).fg(Color::Green),
        "running" | "in_progress" => Cell::new(status).fg(Color::Blue),
        "pending" | "queued" => Cell::new(status).fg(Color::Yellow),
        "failed" | "error" => Cell::new(status).fg(Color::Red),
        _ => Cell::new(status),
    }
}

/// Format duration in human-readable form
pub fn format_duration(seconds: i64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

/// Format bytes in human-readable form
pub fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;
    const TB: i64 = GB * 1024;

    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes < TB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    }
}

/// Format count in human-readable form
pub fn format_count(count: i64) -> String {
    if count < 1000 {
        count.to_string()
    } else if count < 1_000_000 {
        format!("{:.1}K", count as f64 / 1000.0)
    } else if count < 1_000_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", count as f64 / 1_000_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // OutputFormat tests
    #[test]
    fn test_output_format_parse_json() {
        assert_eq!(OutputFormat::parse("json"), OutputFormat::Json);
        assert_eq!(OutputFormat::parse("JSON"), OutputFormat::Json);
        assert_eq!(OutputFormat::parse("Json"), OutputFormat::Json);
    }

    #[test]
    fn test_output_format_parse_yaml() {
        assert_eq!(OutputFormat::parse("yaml"), OutputFormat::Yaml);
        assert_eq!(OutputFormat::parse("YAML"), OutputFormat::Yaml);
        assert_eq!(OutputFormat::parse("yml"), OutputFormat::Yaml);
        assert_eq!(OutputFormat::parse("YML"), OutputFormat::Yaml);
    }

    #[test]
    fn test_output_format_parse_table_default() {
        assert_eq!(OutputFormat::parse("table"), OutputFormat::Table);
        assert_eq!(OutputFormat::parse("TABLE"), OutputFormat::Table);
        assert_eq!(OutputFormat::parse(""), OutputFormat::Table);
        assert_eq!(OutputFormat::parse("unknown"), OutputFormat::Table);
        assert_eq!(OutputFormat::parse("csv"), OutputFormat::Table);
    }

    // format_duration tests
    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(1), "1s");
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(59), "59s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(60), "1m 0s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(125), "2m 5s");
        assert_eq!(format_duration(3599), "59m 59s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(3600), "1h 0m");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(7200), "2h 0m");
        assert_eq!(format_duration(7320), "2h 2m");
        assert_eq!(format_duration(86400), "24h 0m");
    }

    // format_bytes tests
    #[test]
    fn test_format_bytes_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_kilobytes() {
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(10240), "10.0 KB");
        assert_eq!(format_bytes(1048575), "1024.0 KB");
    }

    #[test]
    fn test_format_bytes_megabytes() {
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1572864), "1.5 MB");
        assert_eq!(format_bytes(104857600), "100.0 MB");
    }

    #[test]
    fn test_format_bytes_gigabytes() {
        assert_eq!(format_bytes(1073741824), "1.0 GB");
        assert_eq!(format_bytes(1610612736), "1.5 GB");
        assert_eq!(format_bytes(10737418240), "10.0 GB");
    }

    #[test]
    fn test_format_bytes_terabytes() {
        assert_eq!(format_bytes(1099511627776), "1.0 TB");
        assert_eq!(format_bytes(1649267441664), "1.5 TB");
    }

    // format_count tests
    #[test]
    fn test_format_count_units() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1), "1");
        assert_eq!(format_count(500), "500");
        assert_eq!(format_count(999), "999");
    }

    #[test]
    fn test_format_count_thousands() {
        assert_eq!(format_count(1000), "1.0K");
        assert_eq!(format_count(1500), "1.5K");
        assert_eq!(format_count(10000), "10.0K");
        assert_eq!(format_count(999999), "1000.0K");
    }

    #[test]
    fn test_format_count_millions() {
        assert_eq!(format_count(1_000_000), "1.0M");
        assert_eq!(format_count(1_500_000), "1.5M");
        assert_eq!(format_count(100_000_000), "100.0M");
        assert_eq!(format_count(999_999_999), "1000.0M");
    }

    #[test]
    fn test_format_count_billions() {
        assert_eq!(format_count(1_000_000_000), "1.0B");
        assert_eq!(format_count(1_500_000_000), "1.5B");
        assert_eq!(format_count(10_000_000_000), "10.0B");
    }

    // format_status tests
    #[test]
    fn test_format_status_success() {
        let cell = format_status("success");
        assert_eq!(cell.content(), "success");

        let cell = format_status("completed");
        assert_eq!(cell.content(), "completed");
    }

    #[test]
    fn test_format_status_running() {
        let cell = format_status("running");
        assert_eq!(cell.content(), "running");

        let cell = format_status("in_progress");
        assert_eq!(cell.content(), "in_progress");
    }

    #[test]
    fn test_format_status_pending() {
        let cell = format_status("pending");
        assert_eq!(cell.content(), "pending");

        let cell = format_status("queued");
        assert_eq!(cell.content(), "queued");
    }

    #[test]
    fn test_format_status_failed() {
        let cell = format_status("failed");
        assert_eq!(cell.content(), "failed");

        let cell = format_status("error");
        assert_eq!(cell.content(), "error");
    }

    #[test]
    fn test_format_status_case_insensitive() {
        let cell = format_status("SUCCESS");
        assert_eq!(cell.content(), "SUCCESS");

        let cell = format_status("Running");
        assert_eq!(cell.content(), "Running");
    }

    #[test]
    fn test_format_status_unknown() {
        let cell = format_status("unknown_status");
        assert_eq!(cell.content(), "unknown_status");
    }

    // create_table tests
    #[test]
    fn test_create_table() {
        let table = create_table();
        // Just verify it creates without panicking
        assert!(table.is_empty());
    }

    // print_output tests
    #[test]
    fn test_print_output_json() {
        #[derive(Serialize)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // This will print to stdout, but we can verify it doesn't error
        let result = print_output(&data, "json");
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_output_yaml() {
        #[derive(Serialize)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = print_output(&data, "yaml");
        assert!(result.is_ok());
    }
}

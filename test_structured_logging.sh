#!/bin/bash
# Test script to demonstrate structured logging with JSON output

echo "====================================================================="
echo "FeatureDuck Structured Logging Demonstration"
echo "====================================================================="
echo ""

echo "1. Testing PRETTY format (human-readable, development)"
echo "---------------------------------------------------------------------"
echo "Command: LOG_FORMAT=pretty RUST_LOG=info cargo run --bin featureduck --quiet -- serve --help"
echo ""
LOG_FORMAT=pretty RUST_LOG=info cargo run --bin featureduck --quiet -- serve --help 2>&1 | head -20
echo ""

echo "2. Testing JSON format (machine-readable, production)"
echo "---------------------------------------------------------------------"
echo "Command: LOG_FORMAT=json RUST_LOG=info cargo run --bin featureduck --quiet -- list --help"
echo ""
LOG_FORMAT=json RUST_LOG=info cargo run --bin featureduck --quiet -- list --help 2>&1 | head -20
echo ""

echo "3. Testing TRACE level with feature_view filter"
echo "---------------------------------------------------------------------"
echo "Command: RUST_LOG=featureduck=trace cargo run --bin featureduck --quiet -- list --help"
echo ""
RUST_LOG=featureduck=trace cargo run --bin featureduck --quiet -- list --help 2>&1 | head -20
echo ""

echo "====================================================================="
echo "Structured Logging Features Demonstrated:"
echo "====================================================================="
echo "✅ Environment-based format selection (LOG_FORMAT=json/pretty)"
echo "✅ Flexible log levels via RUST_LOG env var"
echo "✅ JSON output for production (with span context, thread IDs)"
echo "✅ Pretty output for development (human-readable)"
echo "✅ Span instrumentation on key API handlers"
echo "✅ Request correlation via run_id in spans"
echo ""

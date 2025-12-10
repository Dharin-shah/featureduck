//! Optimized write operations with parallel processing and compression tuning
//!
//! This module provides performance-optimized write operations that:
//! 1. Build RecordBatches in parallel using rayon
//! 2. Support configurable Parquet compression
//! 3. Cache schema inference for reuse
//! 4. Write multiple Parquet files concurrently

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use featureduck_core::{FeatureRow, FeatureValue, Result};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Compression codec for Parquet files
///
/// ZSTD is recommended for feature stores as it provides excellent
/// compression ratios while maintaining fast decompression speeds.
///
/// ## Compression Comparison (typical feature data):
/// | Codec        | Ratio | Write Speed | Read Speed | Use Case         |
/// |--------------|-------|-------------|------------|------------------|
/// | Uncompressed | 1x    | Fastest     | Fastest    | Temporary/debug  |
/// | Snappy       | 2-3x  | Very Fast   | Very Fast  | Legacy compat    |
/// | LZ4          | 2-3x  | Very Fast   | Very Fast  | Low latency      |
/// | ZstdLevel1   | 3-4x  | Fast        | Very Fast  | Default balance  |
/// | ZstdLevel3   | 4-5x  | Medium      | Very Fast  | Good balance     |
/// | ZstdLevel9   | 5-7x  | Slow        | Fast       | Max compression  |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression (fastest writes, largest files)
    Uncompressed,
    /// Snappy compression (good compatibility)
    Snappy,
    /// ZSTD level 1 (fast compression, good ratio) - DEFAULT
    ZstdLevel1,
    /// ZSTD level 3 (balanced compression)
    ZstdLevel3,
    /// ZSTD level 9 (maximum compression, slower writes)
    ZstdLevel9,
    /// LZ4 (very fast compression and decompression)
    Lz4,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        // ZSTD Level 3 is a good default - better compression than level 1
        // with minimal impact on write speed
        Self::ZstdLevel3
    }
}

impl CompressionCodec {
    /// Get codec optimized for maximum write speed
    pub fn fastest() -> Self {
        Self::Lz4
    }

    /// Get codec optimized for maximum compression
    pub fn smallest() -> Self {
        Self::ZstdLevel9
    }

    /// Get codec optimized for balanced performance
    pub fn balanced() -> Self {
        Self::ZstdLevel3
    }
}

/// Configuration for optimized writes
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// Compression codec for Parquet files
    pub compression: CompressionCodec,

    /// Chunk size for parallel processing (rows per chunk)
    /// Smaller chunks = more parallelism but more overhead
    /// Larger chunks = less parallelism but less overhead
    /// Default: 100,000 rows
    pub chunk_size: usize,

    /// Enable parallel RecordBatch construction
    pub parallel_enabled: bool,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            compression: CompressionCodec::default(),
            chunk_size: 100_000,
            parallel_enabled: true,
        }
    }
}

/// Inferred schema from FeatureRows
/// This is cached to avoid repeated schema inference
#[derive(Debug, Clone)]
pub struct InferredSchema {
    pub schema: Arc<Schema>,
    pub entity_names: Vec<String>,
    pub feature_names: Vec<String>,
    pub feature_types: HashMap<String, DataType>,
}

impl InferredSchema {
    /// Infer schema from the first row
    pub fn from_rows(rows: &[FeatureRow]) -> Result<Self> {
        if rows.is_empty() {
            return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Cannot infer schema from empty rows"
            )));
        }

        let first_row = &rows[0];
        let entity_names: Vec<String> = first_row.entities.iter().map(|e| e.name.clone()).collect();

        let feature_names: Vec<String> = first_row.features.keys().cloned().collect();

        // Build feature type map
        let mut feature_types = HashMap::new();
        for feature_name in &feature_names {
            let feature_value = first_row.features.get(feature_name).ok_or_else(|| {
                featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Feature '{}' not found in first row during schema inference",
                    feature_name
                ))
            })?;

            let data_type = match feature_value {
                FeatureValue::Int(_) => DataType::Int64,
                FeatureValue::Float(_) => DataType::Float64,
                FeatureValue::String(_) => DataType::Utf8,
                FeatureValue::Bool(_) => DataType::Boolean,
                FeatureValue::Null => DataType::Utf8,
                // Advanced types
                FeatureValue::Json(_) => DataType::Utf8, // Store JSON as string
                FeatureValue::ArrayInt(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                }
                FeatureValue::ArrayFloat(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                }
                FeatureValue::ArrayString(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                }
                FeatureValue::Date(_) => DataType::Date32, // Arrow Date32 for date-only values
            };
            feature_types.insert(feature_name.clone(), data_type);
        }

        // Build Arrow schema
        // Pre-allocate: entities + features + timestamp column
        let mut fields = Vec::with_capacity(entity_names.len() + feature_names.len() + 1);

        // Entity columns
        for entity_name in &entity_names {
            fields.push(Field::new(entity_name, DataType::Utf8, false));
        }

        // Feature columns
        for feature_name in &feature_names {
            let data_type = feature_types
                .get(feature_name)
                .ok_or_else(|| {
                    featureduck_core::Error::StorageError(anyhow::anyhow!(
                        "Feature type for '{}' not found in schema",
                        feature_name
                    ))
                })?
                .clone();
            fields.push(Field::new(feature_name, data_type, true));
        }

        // Timestamp column (required for point-in-time)
        fields.push(Field::new(
            "__timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));

        let schema = Arc::new(Schema::new(fields));

        Ok(InferredSchema {
            schema,
            entity_names,
            feature_names,
            feature_types,
        })
    }
}

/// Convert FeatureRows to RecordBatch using pre-inferred schema
/// This is the core conversion function used by both serial and parallel paths
fn rows_to_record_batch_with_schema(
    rows: &[FeatureRow],
    schema_info: &InferredSchema,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
            "Cannot create RecordBatch from empty rows"
        )));
    }

    // Pre-allocate: entities + features + timestamp column
    let mut columns: Vec<ArrayRef> =
        Vec::with_capacity(schema_info.entity_names.len() + schema_info.feature_names.len() + 1);

    // Build entity columns
    for entity_name in &schema_info.entity_names {
        let values: Vec<String> = rows
            .iter()
            .map(|row| {
                row.entities
                    .iter()
                    .find(|e| &e.name == entity_name)
                    .map(|e| e.value.clone())
                    .unwrap_or_default()
            })
            .collect();
        columns.push(Arc::new(StringArray::from(values)));
    }

    // Build feature columns
    for feature_name in &schema_info.feature_names {
        let data_type = schema_info.feature_types.get(feature_name).ok_or_else(|| {
            featureduck_core::Error::StorageError(anyhow::anyhow!(
                "Feature type for '{}' not found during batch conversion",
                feature_name
            ))
        })?;

        let array: ArrayRef = match data_type {
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| match row.features.get(feature_name) {
                        Some(FeatureValue::Int(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| match row.features.get(feature_name) {
                        Some(FeatureValue::Float(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Utf8 => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| match row.features.get(feature_name) {
                        Some(FeatureValue::String(v)) => Some(v.clone()),
                        // JSON is stored as string
                        Some(FeatureValue::Json(v)) => {
                            Some(serde_json::to_string(v).unwrap_or_default())
                        }
                        _ => None,
                    })
                    .collect();
                Arc::new(StringArray::from(values))
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| match row.features.get(feature_name) {
                        Some(FeatureValue::Bool(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Date32 => {
                use arrow::array::Date32Array;
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| match row.features.get(feature_name) {
                        Some(FeatureValue::Date(date)) => {
                            // Convert NaiveDate to days since Unix epoch
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days = date.signed_duration_since(epoch).num_days();
                            Some(days as i32)
                        }
                        _ => None,
                    })
                    .collect();
                Arc::new(Date32Array::from(values))
            }
            DataType::List(field) => {
                use arrow::array::ListBuilder;

                // Determine the item type
                match field.data_type() {
                    DataType::Int64 => {
                        use arrow::array::Int64Builder;
                        let mut builder = ListBuilder::new(Int64Builder::new());

                        for row in rows {
                            match row.features.get(feature_name) {
                                Some(FeatureValue::ArrayInt(arr)) => {
                                    for &val in arr {
                                        builder.values().append_value(val);
                                    }
                                    builder.append(true);
                                }
                                _ => builder.append(false),
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Float64 => {
                        use arrow::array::Float64Builder;
                        let mut builder = ListBuilder::new(Float64Builder::new());

                        for row in rows {
                            match row.features.get(feature_name) {
                                Some(FeatureValue::ArrayFloat(arr)) => {
                                    for &val in arr {
                                        builder.values().append_value(val);
                                    }
                                    builder.append(true);
                                }
                                _ => builder.append(false),
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    DataType::Utf8 => {
                        use arrow::array::StringBuilder;
                        let mut builder = ListBuilder::new(StringBuilder::new());

                        for row in rows {
                            match row.features.get(feature_name) {
                                Some(FeatureValue::ArrayString(arr)) => {
                                    for val in arr {
                                        builder.values().append_value(val);
                                    }
                                    builder.append(true);
                                }
                                _ => builder.append(false),
                            }
                        }
                        Arc::new(builder.finish())
                    }
                    _ => {
                        return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                            "Unsupported list item type: {:?}",
                            field.data_type()
                        )));
                    }
                }
            }
            _ => {
                return Err(featureduck_core::Error::StorageError(anyhow::anyhow!(
                    "Unsupported data type: {:?}",
                    data_type
                )));
            }
        };
        columns.push(array);
    }

    // Add timestamp column
    let timestamps: Vec<i64> = rows
        .iter()
        .map(|row| row.timestamp.timestamp_micros())
        .collect();
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"),
    ));

    RecordBatch::try_new(schema_info.schema.clone(), columns)
        .map_err(|e| featureduck_core::Error::StorageError(e.into()))
}

/// Convert FeatureRows to RecordBatches with parallel processing
///
/// This function:
/// 1. Infers schema once from the first row (cached)
/// 2. Splits rows into chunks
/// 3. Builds RecordBatches in parallel using rayon
/// 4. Returns multiple RecordBatches (one per chunk)
pub fn rows_to_record_batches_parallel(
    rows: Vec<FeatureRow>,
    config: &WriteConfig,
) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        return Ok(vec![]);
    }

    // Infer schema once
    let schema_info = InferredSchema::from_rows(&rows)?;

    // If parallel is disabled or rows are small, use serial path
    if !config.parallel_enabled || rows.len() < config.chunk_size {
        let batch = rows_to_record_batch_with_schema(&rows, &schema_info)?;
        return Ok(vec![batch]);
    }

    // Split into chunks and process in parallel
    // Use into_iter() to consume rows and avoid unnecessary copies
    let chunk_size = config.chunk_size;
    let num_chunks = rows.len().div_ceil(chunk_size);
    let mut chunks = Vec::with_capacity(num_chunks);

    let mut iter = rows.into_iter();
    while let Some(first) = iter.next() {
        let mut chunk = Vec::with_capacity(chunk_size);
        chunk.push(first);
        chunk.extend(iter.by_ref().take(chunk_size - 1));
        chunks.push(chunk);
    }

    let batches: Result<Vec<RecordBatch>> = chunks
        .par_iter()
        .map(|chunk| rows_to_record_batch_with_schema(chunk, &schema_info))
        .collect();

    batches
}

/// Convert FeatureRows to a single RecordBatch (serial, for backward compatibility)
#[allow(dead_code)]
pub fn rows_to_record_batch(rows: &[FeatureRow]) -> Result<RecordBatch> {
    let schema_info = InferredSchema::from_rows(rows)?;
    rows_to_record_batch_with_schema(rows, &schema_info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use featureduck_core::EntityKey;

    fn create_test_rows(count: usize) -> Vec<FeatureRow> {
        (0..count)
            .map(|i| {
                let mut row = FeatureRow::new(
                    vec![EntityKey::new("user_id", format!("user_{}", i))],
                    Utc::now(),
                );
                row.add_feature("clicks".to_string(), FeatureValue::Int(i as i64));
                row.add_feature("score".to_string(), FeatureValue::Float(i as f64 * 0.5));
                row.add_feature("active".to_string(), FeatureValue::Bool(i % 2 == 0));
                row
            })
            .collect()
    }

    #[test]
    fn test_infer_schema() {
        let rows = create_test_rows(10);
        let schema = InferredSchema::from_rows(&rows).unwrap();

        assert_eq!(schema.entity_names, vec!["user_id"]);
        assert_eq!(schema.feature_names.len(), 3);
        assert!(schema.feature_names.contains(&"clicks".to_string()));
        assert_eq!(schema.feature_types.get("clicks"), Some(&DataType::Int64));
        assert_eq!(schema.feature_types.get("score"), Some(&DataType::Float64));
        assert_eq!(schema.feature_types.get("active"), Some(&DataType::Boolean));
    }

    #[test]
    fn test_rows_to_record_batch() {
        let rows = create_test_rows(100);
        let batch = rows_to_record_batch(&rows).unwrap();

        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 5); // 1 entity + 3 features + 1 timestamp
    }

    #[test]
    fn test_parallel_small_dataset() {
        let rows = create_test_rows(50);
        let config = WriteConfig::default();

        let batches = rows_to_record_batches_parallel(rows, &config).unwrap();

        // Small dataset should produce 1 batch
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 50);
    }

    #[test]
    fn test_parallel_large_dataset() {
        let rows = create_test_rows(250_000);
        let config = WriteConfig {
            chunk_size: 100_000,
            ..Default::default()
        };

        let batches = rows_to_record_batches_parallel(rows, &config).unwrap();

        // Should split into 3 chunks: 100k, 100k, 50k
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 100_000);
        assert_eq!(batches[1].num_rows(), 100_000);
        assert_eq!(batches[2].num_rows(), 50_000);

        // Verify total rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 250_000);
    }

    #[test]
    fn test_parallel_disabled() {
        let rows = create_test_rows(200_000);
        let config = WriteConfig {
            parallel_enabled: false,
            ..Default::default()
        };

        let batches = rows_to_record_batches_parallel(rows, &config).unwrap();

        // Parallel disabled, should produce 1 batch
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 200_000);
    }

    #[test]
    fn test_schema_consistency_across_chunks() {
        let rows = create_test_rows(150_000);
        let config = WriteConfig {
            chunk_size: 50_000,
            ..Default::default()
        };

        let batches = rows_to_record_batches_parallel(rows, &config).unwrap();

        assert_eq!(batches.len(), 3);

        // All batches should have the same schema
        let first_schema = batches[0].schema();
        for batch in &batches[1..] {
            assert_eq!(batch.schema(), first_schema);
        }
    }

    #[test]
    fn test_empty_rows() {
        let rows = Vec::new();
        let config = WriteConfig::default();

        let batches = rows_to_record_batches_parallel(rows, &config).unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn test_data_integrity() {
        let rows = create_test_rows(1000);
        let config = WriteConfig {
            chunk_size: 300,
            ..Default::default()
        };

        let batches = rows_to_record_batches_parallel(rows.clone(), &config).unwrap();

        // Verify first and last row values
        let first_batch = &batches[0];
        let last_batch = &batches[batches.len() - 1];

        // Check first row (user_0)
        let user_col = first_batch.column_by_name("user_id").unwrap();
        let user_col = user_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(user_col.value(0), "user_0");

        let clicks_col = first_batch.column_by_name("clicks").unwrap();
        let clicks_col = clicks_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(clicks_col.value(0), 0);

        // Check last row (user_999)
        let last_user_col = last_batch.column_by_name("user_id").unwrap();
        let last_user_col = last_user_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let last_row_idx = last_batch.num_rows() - 1;
        assert_eq!(last_user_col.value(last_row_idx), "user_999");

        let last_clicks_col = last_batch.column_by_name("clicks").unwrap();
        let last_clicks_col = last_clicks_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(last_clicks_col.value(last_row_idx), 999);
    }
}

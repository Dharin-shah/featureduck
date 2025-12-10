//! Incremental aggregation with mathematically correct merge semantics
//!
//! This module provides merge functions for incremental feature computation.
//! By maintaining previous aggregate values, we can update features without
//! re-scanning all historical data.
//!
//! ## Merge Semantics
//!
//! Different aggregation functions have different merge properties:
//!
//! - **COUNT**: Additive - `merge_count(old, new) = old + new`
//! - **SUM**: Additive - `merge_sum(old, new) = old + new`
//! - **AVG**: Derived - Requires COUNT + SUM, then `total_sum / total_count`
//! - **MIN**: Idempotent - `merge_min(old, new) = MIN(old, new)`
//! - **MAX**: Idempotent - `merge_max(old, new) = MAX(old, new)`
//! - **COUNT DISTINCT**: Complex - Requires HyperLogLog or exact set (not implemented yet)
//!
//! ## Example
//!
//! ```rust
//! use featureduck_core::incremental::merge_feature_rows;
//! use featureduck_core::{AggExpr, AggFunction, FeatureRow, FeatureValue, EntityKey};
//! use chrono::Utc;
//!
//! let entity = EntityKey::new("user_id", "user_1");
//!
//! // Old state: 100 clicks
//! let mut old_row = FeatureRow::new(vec![entity.clone()], Utc::now());
//! old_row.add_feature("clicks".to_string(), FeatureValue::Int(100));
//!
//! // New aggregate: 50 clicks
//! let mut new_row = FeatureRow::new(vec![entity.clone()], Utc::now());
//! new_row.add_feature("clicks".to_string(), FeatureValue::Int(50));
//!
//! let aggregations = vec![AggExpr {
//!     function: AggFunction::CountAll,
//!     alias: Some("clicks".to_string()),
//! }];
//!
//! let merged = merge_feature_rows(&old_row, &new_row, &aggregations).unwrap();
//! assert_eq!(merged.get_feature("clicks"), Some(&FeatureValue::Int(150)));
//! ```

use crate::{AggExpr, AggFunction, FeatureRow, FeatureValue, Result};
use std::collections::HashMap;

/// Merge two FeatureRows based on aggregation semantics
///
/// **Memory Optimization (GAP #4):** Takes ownership of `new_row` to avoid 24-72 MB of clones
/// for large materializations. This enables move semantics instead of expensive memory copies.
///
/// # Arguments
/// * `old_row` - Previous aggregated state (borrowed, read-only)
/// * `new_row` - Newly computed aggregates from incremental data (consumed, moved)
/// * `aggregations` - Aggregation definitions to determine merge strategy
///
/// # Returns
/// Merged FeatureRow with correct semantics for each aggregation type
///
/// # Example
/// ```rust
/// use featureduck_core::incremental::merge_feature_rows;
/// use featureduck_core::{AggExpr, AggFunction, Expr, FeatureRow, FeatureValue, EntityKey};
/// use chrono::Utc;
///
/// let entity = EntityKey::new("user_id", "user_1");
///
/// let mut old_row = FeatureRow::new(vec![entity.clone()], Utc::now());
/// old_row.add_feature("total".to_string(), FeatureValue::Int(1000));
///
/// let mut new_row = FeatureRow::new(vec![entity.clone()], Utc::now());
/// new_row.add_feature("total".to_string(), FeatureValue::Int(500));
///
/// let aggregations = vec![AggExpr {
///     function: AggFunction::Sum(Expr::column("amount")),
///     alias: Some("total".to_string()),
/// }];
///
/// let merged = merge_feature_rows(&old_row, new_row, &aggregations).unwrap();
/// assert_eq!(merged.get_feature("total"), Some(&FeatureValue::Int(1500)));
/// ```
pub fn merge_feature_rows(
    old_row: &FeatureRow,
    new_row: FeatureRow,
    aggregations: &[AggExpr],
) -> Result<FeatureRow> {
    // Pre-allocate HashMap with exact capacity
    let mut merged_features = HashMap::with_capacity(aggregations.len());

    for agg in aggregations {
        let feature_name = agg
            .alias
            .as_ref()
            .ok_or_else(|| crate::Error::InvalidInput("Aggregation must have alias".to_string()))?;

        let old_value = old_row.features.get(feature_name);
        let new_value = new_row.features.get(feature_name);

        let merged_value = match &agg.function {
            AggFunction::CountAll | AggFunction::Count(_) => merge_count(old_value, new_value)?,
            AggFunction::Sum(_) => merge_sum(old_value, new_value)?,
            AggFunction::Avg(_) => {
                // AVG requires special handling - we need to maintain count and sum
                // For now, we'll treat new_value as the new average and recompute
                // In a full implementation, we'd store metadata about counts
                merge_avg(
                    old_value,
                    new_value,
                    &old_row.features,
                    &new_row.features,
                    feature_name,
                )?
            }
            AggFunction::Min(_) => merge_min(old_value, new_value)?,
            AggFunction::Max(_) => merge_max(old_value, new_value)?,
            AggFunction::CountDistinct(_) => {
                // COUNT DISTINCT uses HyperLogLog for incremental merge
                // The HLL state is stored as JSON in the metadata field
                merge_count_distinct(
                    old_value,
                    new_value,
                    &old_row.features,
                    &new_row.features,
                    feature_name,
                )?
            }
        };

        merged_features.insert(feature_name.clone(), merged_value);
    }

    // Move entities from new_row (no clone needed!)
    Ok(FeatureRow {
        entities: new_row.entities,
        features: merged_features,
        timestamp: chrono::Utc::now(),
    })
}

/// Merge COUNT aggregations (additive)
fn merge_count(old: Option<&FeatureValue>, new: Option<&FeatureValue>) -> Result<FeatureValue> {
    match (old, new) {
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Int(n))) => Ok(FeatureValue::Int(o + n)),
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(o + n))
        }
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(*o as f64 + n))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Float(o + *n as f64))
        }
        (None, Some(n)) => Ok(n.clone()),
        (Some(o), None) => Ok(o.clone()),
        (None, None) => Ok(FeatureValue::Null),
        _ => Err(crate::Error::InvalidInput(format!(
            "Cannot merge COUNT: {:?} and {:?}",
            old, new
        ))),
    }
}

/// Merge SUM aggregations (additive)
fn merge_sum(old: Option<&FeatureValue>, new: Option<&FeatureValue>) -> Result<FeatureValue> {
    match (old, new) {
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Int(n))) => Ok(FeatureValue::Int(o + n)),
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(o + n))
        }
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(*o as f64 + n))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Float(o + *n as f64))
        }
        (None, Some(n)) => Ok(n.clone()),
        (Some(o), None) => Ok(o.clone()),
        (None, None) => Ok(FeatureValue::Null),
        _ => Err(crate::Error::InvalidInput(format!(
            "Cannot merge SUM: {:?} and {:?}",
            old, new
        ))),
    }
}

/// Merge AVG aggregations (requires recomputation from SUM and COUNT)
///
/// AVG is not directly mergeable. We need to maintain SUM and COUNT separately.
/// This function looks for companion features: `{feature}_sum` and `{feature}_count`
fn merge_avg(
    old: Option<&FeatureValue>,
    new: Option<&FeatureValue>,
    old_features: &HashMap<String, FeatureValue>,
    new_features: &HashMap<String, FeatureValue>,
    feature_name: &str,
) -> Result<FeatureValue> {
    // Try to find companion SUM and COUNT features
    let sum_key = format!(
        "{}_sum",
        feature_name.strip_suffix("_avg").unwrap_or(feature_name)
    );
    let count_key = format!(
        "{}_count",
        feature_name.strip_suffix("_avg").unwrap_or(feature_name)
    );

    let old_sum = old_features.get(&sum_key);
    let old_count = old_features.get(&count_key);
    let new_sum = new_features.get(&sum_key);
    let new_count = new_features.get(&count_key);

    if let (Some(os), Some(oc), Some(ns), Some(nc)) = (old_sum, old_count, new_sum, new_count) {
        // We have metadata, compute correctly
        let merged_sum = merge_sum(Some(os), Some(ns))?;
        let merged_count = merge_count(Some(oc), Some(nc))?;

        match (merged_sum, merged_count) {
            (FeatureValue::Float(s), FeatureValue::Int(c)) if c > 0 => {
                Ok(FeatureValue::Float(s / c as f64))
            }
            (FeatureValue::Int(s), FeatureValue::Int(c)) if c > 0 => {
                Ok(FeatureValue::Float(s as f64 / c as f64))
            }
            _ => Ok(FeatureValue::Null),
        }
    } else {
        // No metadata, use simple weighted average
        // This is less accurate but better than nothing
        match (old, new, old_count, new_count) {
            (
                Some(FeatureValue::Float(o)),
                Some(FeatureValue::Float(n)),
                Some(FeatureValue::Int(oc)),
                Some(FeatureValue::Int(nc)),
            ) => {
                let total_count = oc + nc;
                if total_count > 0 {
                    Ok(FeatureValue::Float(
                        (o * (*oc as f64) + n * (*nc as f64)) / (total_count as f64),
                    ))
                } else {
                    Ok(FeatureValue::Null)
                }
            }
            (None, Some(n), _, _) => Ok(n.clone()),
            (Some(o), None, _, _) => Ok(o.clone()),
            _ => {
                // Fallback: just use new value (not ideal but safe)
                Ok(new.cloned().unwrap_or(FeatureValue::Null))
            }
        }
    }
}

/// Merge MIN aggregations (idempotent)
fn merge_min(old: Option<&FeatureValue>, new: Option<&FeatureValue>) -> Result<FeatureValue> {
    match (old, new) {
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Int(*o.min(n)))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(o.min(*n)))
        }
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float((*o as f64).min(*n)))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Float(o.min(*n as f64)))
        }
        (None, Some(n)) => Ok(n.clone()),
        (Some(o), None) => Ok(o.clone()),
        (None, None) => Ok(FeatureValue::Null),
        _ => Err(crate::Error::InvalidInput(format!(
            "Cannot merge MIN: {:?} and {:?}",
            old, new
        ))),
    }
}

/// Merge MAX aggregations (idempotent)
fn merge_max(old: Option<&FeatureValue>, new: Option<&FeatureValue>) -> Result<FeatureValue> {
    match (old, new) {
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Int(*o.max(n)))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float(o.max(*n)))
        }
        (Some(FeatureValue::Int(o)), Some(FeatureValue::Float(n))) => {
            Ok(FeatureValue::Float((*o as f64).max(*n)))
        }
        (Some(FeatureValue::Float(o)), Some(FeatureValue::Int(n))) => {
            Ok(FeatureValue::Float(o.max(*n as f64)))
        }
        (None, Some(n)) => Ok(n.clone()),
        (Some(o), None) => Ok(o.clone()),
        (None, None) => Ok(FeatureValue::Null),
        _ => Err(crate::Error::InvalidInput(format!(
            "Cannot merge MAX: {:?} and {:?}",
            old, new
        ))),
    }
}

/// Merge COUNT DISTINCT aggregations using HyperLogLog
///
/// HyperLogLog state is stored in a companion field: `{feature}_hll`
/// The feature value contains the estimated cardinality (Int).
///
/// # Merge Strategy
/// 1. Look for HLL state in `{feature}_hll` metadata field
/// 2. If both exist, merge HLLs and update cardinality estimate
/// 3. If only new exists, use new (first materialization)
/// 4. If neither has HLL state, fall back to new value (not truly incremental)
fn merge_count_distinct(
    old_value: Option<&FeatureValue>,
    new_value: Option<&FeatureValue>,
    old_features: &HashMap<String, FeatureValue>,
    new_features: &HashMap<String, FeatureValue>,
    feature_name: &str,
) -> Result<FeatureValue> {
    use crate::hyperloglog::HyperLogLog;

    let hll_key = format!("{}_hll", feature_name);

    // Try to get HLL state from metadata
    let old_hll_bytes = old_features.get(&hll_key).and_then(|v| {
        if let FeatureValue::String(s) = v {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
        } else {
            None
        }
    });

    let new_hll_bytes = new_features.get(&hll_key).and_then(|v| {
        if let FeatureValue::String(s) = v {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
        } else {
            None
        }
    });

    match (old_hll_bytes, new_hll_bytes) {
        (Some(old_bytes), Some(new_bytes)) => {
            // Both have HLL state - merge them
            let mut old_hll = HyperLogLog::from_bytes(&old_bytes)
                .map_err(|e| crate::Error::InvalidInput(format!("Invalid HLL state: {}", e)))?;
            let new_hll = HyperLogLog::from_bytes(&new_bytes)
                .map_err(|e| crate::Error::InvalidInput(format!("Invalid HLL state: {}", e)))?;

            old_hll.merge(&new_hll);
            Ok(FeatureValue::Int(old_hll.cardinality() as i64))
        }
        (None, Some(_)) => {
            // Only new has HLL state - use new value
            Ok(new_value.cloned().unwrap_or(FeatureValue::Null))
        }
        (Some(_), None) => {
            // Only old has HLL state - keep old value
            Ok(old_value.cloned().unwrap_or(FeatureValue::Null))
        }
        (None, None) => {
            // Neither has HLL state - fall back to simple merge
            // This happens when HLL wasn't enabled during materialization
            // Just use the new value (not incremental, but safe)
            Ok(new_value
                .cloned()
                .unwrap_or_else(|| old_value.cloned().unwrap_or(FeatureValue::Null)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityKey, Expr};
    use chrono::Utc;

    #[test]
    fn test_merge_count() {
        // Int + Int
        let result =
            merge_count(Some(&FeatureValue::Int(10)), Some(&FeatureValue::Int(5))).unwrap();
        assert_eq!(result, FeatureValue::Int(15));

        // Float + Float
        let result = merge_count(
            Some(&FeatureValue::Float(10.0)),
            Some(&FeatureValue::Float(5.5)),
        )
        .unwrap();
        assert_eq!(result, FeatureValue::Float(15.5));

        // None + Some
        let result = merge_count(None, Some(&FeatureValue::Int(5))).unwrap();
        assert_eq!(result, FeatureValue::Int(5));

        // Some + None
        let result = merge_count(Some(&FeatureValue::Int(10)), None).unwrap();
        assert_eq!(result, FeatureValue::Int(10));
    }

    #[test]
    fn test_merge_sum() {
        let result =
            merge_sum(Some(&FeatureValue::Int(100)), Some(&FeatureValue::Int(50))).unwrap();
        assert_eq!(result, FeatureValue::Int(150));

        let result = merge_sum(
            Some(&FeatureValue::Float(100.5)),
            Some(&FeatureValue::Float(50.3)),
        )
        .unwrap();
        assert_eq!(result, FeatureValue::Float(150.8));
    }

    #[test]
    fn test_merge_min() {
        // Int min
        let result = merge_min(Some(&FeatureValue::Int(10)), Some(&FeatureValue::Int(5))).unwrap();
        assert_eq!(result, FeatureValue::Int(5));

        let result = merge_min(Some(&FeatureValue::Int(5)), Some(&FeatureValue::Int(10))).unwrap();
        assert_eq!(result, FeatureValue::Int(5));

        // Float min
        let result = merge_min(
            Some(&FeatureValue::Float(10.5)),
            Some(&FeatureValue::Float(5.3)),
        )
        .unwrap();
        assert_eq!(result, FeatureValue::Float(5.3));

        // None handling
        let result = merge_min(None, Some(&FeatureValue::Int(5))).unwrap();
        assert_eq!(result, FeatureValue::Int(5));
    }

    #[test]
    fn test_merge_max() {
        // Int max
        let result = merge_max(Some(&FeatureValue::Int(10)), Some(&FeatureValue::Int(5))).unwrap();
        assert_eq!(result, FeatureValue::Int(10));

        let result = merge_max(Some(&FeatureValue::Int(5)), Some(&FeatureValue::Int(10))).unwrap();
        assert_eq!(result, FeatureValue::Int(10));

        // Float max
        let result = merge_max(
            Some(&FeatureValue::Float(10.5)),
            Some(&FeatureValue::Float(5.3)),
        )
        .unwrap();
        assert_eq!(result, FeatureValue::Float(10.5));
    }

    #[test]
    fn test_merge_feature_rows_count() {
        let entity = EntityKey::new("user_id", "user_1");

        let mut old_row = FeatureRow::new(vec![entity.clone()], Utc::now());
        old_row.add_feature("event_count".to_string(), FeatureValue::Int(100));

        let mut new_row = FeatureRow::new(vec![entity.clone()], Utc::now());
        new_row.add_feature("event_count".to_string(), FeatureValue::Int(50));

        let aggregations = vec![AggExpr {
            function: AggFunction::CountAll,
            alias: Some("event_count".to_string()),
        }];

        // Pass new_row by value (move semantics)
        let merged = merge_feature_rows(&old_row, new_row, &aggregations).unwrap();

        assert_eq!(
            merged.features.get("event_count"),
            Some(&FeatureValue::Int(150))
        );
    }

    #[test]
    fn test_merge_feature_rows_multiple() {
        let entity = EntityKey::new("user_id", "user_1");

        let mut old_row = FeatureRow::new(vec![entity.clone()], Utc::now());
        old_row.add_feature("event_count".to_string(), FeatureValue::Int(100));
        old_row.add_feature("total_amount".to_string(), FeatureValue::Int(1000));
        old_row.add_feature("min_amount".to_string(), FeatureValue::Int(10));
        old_row.add_feature("max_amount".to_string(), FeatureValue::Int(100));

        let mut new_row = FeatureRow::new(vec![entity.clone()], Utc::now());
        new_row.add_feature("event_count".to_string(), FeatureValue::Int(50));
        new_row.add_feature("total_amount".to_string(), FeatureValue::Int(500));
        new_row.add_feature("min_amount".to_string(), FeatureValue::Int(5));
        new_row.add_feature("max_amount".to_string(), FeatureValue::Int(80));

        let aggregations = vec![
            AggExpr {
                function: AggFunction::CountAll,
                alias: Some("event_count".to_string()),
            },
            AggExpr {
                function: AggFunction::Sum(Expr::column("amount")),
                alias: Some("total_amount".to_string()),
            },
            AggExpr {
                function: AggFunction::Min(Expr::column("amount")),
                alias: Some("min_amount".to_string()),
            },
            AggExpr {
                function: AggFunction::Max(Expr::column("amount")),
                alias: Some("max_amount".to_string()),
            },
        ];

        // Pass new_row by value (move semantics)
        let merged = merge_feature_rows(&old_row, new_row, &aggregations).unwrap();

        assert_eq!(
            merged.features.get("event_count"),
            Some(&FeatureValue::Int(150))
        );
        assert_eq!(
            merged.features.get("total_amount"),
            Some(&FeatureValue::Int(1500))
        );
        assert_eq!(
            merged.features.get("min_amount"),
            Some(&FeatureValue::Int(5))
        );
        assert_eq!(
            merged.features.get("max_amount"),
            Some(&FeatureValue::Int(100))
        );
    }
}

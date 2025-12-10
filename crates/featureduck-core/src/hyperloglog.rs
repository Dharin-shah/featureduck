//! HyperLogLog Implementation for Approximate COUNT DISTINCT
//!
//! HyperLogLog is a probabilistic data structure for estimating cardinality
//! (count distinct) with sub-linear space complexity.
//!
//! ## Why HyperLogLog?
//!
//! - **Space Efficient**: O(log log n) space vs O(n) for exact count
//! - **Mergeable**: Multiple HLLs can be merged (union operation)
//! - **Fast**: O(1) insertion, O(m) cardinality estimation
//! - **Configurable Precision**: Trade accuracy for space
//!
//! ## Precision vs Memory vs Error
//!
//! | Precision | Registers | Memory | Std Error |
//! |-----------|-----------|--------|-----------|
//! | 10        | 1,024     | 1 KB   | 3.25%     |
//! | 12        | 4,096     | 4 KB   | 1.62%     |
//! | 14        | 16,384    | 16 KB  | 0.81%     |
//! | 16        | 65,536    | 64 KB  | 0.40%     |
//!
//! ## Algorithm
//!
//! 1. Hash each element to 64-bit value
//! 2. Use first `p` bits to select register (bucket)
//! 3. Count leading zeros in remaining bits + 1
//! 4. Store maximum count in each register
//! 5. Estimate cardinality using harmonic mean
//!
//! ## Example
//!
//! ```rust
//! use featureduck_core::hyperloglog::HyperLogLog;
//!
//! let mut hll = HyperLogLog::new(12); // ~1.6% error
//!
//! // Add elements
//! for i in 0..10000 {
//!     hll.insert(&format!("user_{}", i));
//! }
//!
//! // Estimate cardinality
//! let estimate = hll.cardinality();
//! println!("Estimated: {}, Actual: 10000", estimate);
//!
//! // Merge two HLLs
//! let mut hll2 = HyperLogLog::new(12);
//! for i in 5000..15000 {
//!     hll2.insert(&format!("user_{}", i));
//! }
//!
//! hll.merge(&hll2);
//! // Now contains union of both sets (~15000 unique elements)
//! ```

use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};

/// HyperLogLog probabilistic cardinality estimator
///
/// Provides approximate COUNT DISTINCT with configurable precision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperLogLog {
    /// Precision parameter (number of bits for register index)
    precision: u8,
    /// Number of registers (2^precision)
    num_registers: usize,
    /// Register array (stores max leading zeros + 1)
    registers: Vec<u8>,
    /// Alpha correction factor (precomputed)
    alpha: f64,
}

impl HyperLogLog {
    /// Create a new HyperLogLog with specified precision
    ///
    /// # Arguments
    /// * `precision` - Number of bits for register selection (4-18 recommended)
    ///
    /// # Precision Guide
    /// - 10: ~3.25% error, 1KB memory
    /// - 12: ~1.62% error, 4KB memory (recommended)
    /// - 14: ~0.81% error, 16KB memory
    /// - 16: ~0.40% error, 64KB memory
    ///
    /// # Panics
    /// Panics if precision is less than 4 or greater than 18
    pub fn new(precision: u8) -> Self {
        assert!(
            (4..=18).contains(&precision),
            "Precision must be between 4 and 18"
        );

        let num_registers = 1 << precision;
        let alpha = Self::compute_alpha(num_registers);

        Self {
            precision,
            num_registers,
            registers: vec![0; num_registers],
            alpha,
        }
    }

    /// Create HyperLogLog with default precision (12 = ~1.6% error)
    pub fn default_precision() -> Self {
        Self::new(12)
    }

    /// Compute alpha correction factor
    fn compute_alpha(m: usize) -> f64 {
        match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m as f64),
        }
    }

    /// Insert a value into the HyperLogLog
    ///
    /// # Arguments
    /// * `value` - Any hashable value
    pub fn insert<T: Hash>(&mut self, value: &T) {
        let hash = self.hash_value(value);
        self.insert_hash(hash);
    }

    /// Insert a pre-computed hash value
    pub fn insert_hash(&mut self, hash: u64) {
        // Use first `precision` bits for register index
        let index = (hash >> (64 - self.precision)) as usize;

        // Count leading zeros in remaining bits + 1
        let remaining_bits = hash << self.precision;
        let zeros = if remaining_bits == 0 {
            64 - self.precision
        } else {
            remaining_bits.leading_zeros() as u8
        };
        let rho = zeros + 1;

        // Store maximum
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    /// Hash a value using SipHash
    fn hash_value<T: Hash>(&self, value: &T) -> u64 {
        let mut hasher = SipHasher13::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Estimate the cardinality (count distinct)
    ///
    /// Returns the estimated number of unique elements inserted.
    /// The standard error is approximately 1.04 / sqrt(m) where m = 2^precision.
    ///
    /// **Optimized:** Uses precomputed lookup table for 2^(-r) to avoid expensive pow() calls.
    #[allow(clippy::excessive_precision)]
    pub fn cardinality(&self) -> u64 {
        // Precomputed powers of 2^(-r) for r=0..64 (avoids expensive powi calls in hot loop)
        const POW2_NEG: [f64; 65] = [
            1.0,
            0.5,
            0.25,
            0.125,
            0.0625,
            0.03125,
            0.015625,
            0.0078125,
            0.00390625,
            0.001953125,
            0.0009765625,
            0.00048828125,
            0.000244140625,
            0.0001220703125,
            6.103515625e-05,
            3.0517578125e-05,
            1.52587890625e-05,
            7.62939453125e-06,
            3.814697265625e-06,
            1.9073486328125e-06,
            9.5367431640625e-07,
            4.76837158203125e-07,
            2.384185791015625e-07,
            1.1920928955078125e-07,
            5.9604644775390625e-08,
            2.9802322387695312e-08,
            1.4901161193847656e-08,
            7.450580596923828e-09,
            3.725290298461914e-09,
            1.862645149230957e-09,
            9.313225746154785e-10,
            4.656612873077393e-10,
            2.3283064365386963e-10,
            1.1641532182693481e-10,
            5.820766091346741e-11,
            2.9103830456733704e-11,
            1.4551915228366852e-11,
            7.275957614183426e-12,
            3.637978807091713e-12,
            1.8189894035458565e-12,
            9.094947017729282e-13,
            4.547473508864641e-13,
            2.2737367544323206e-13,
            1.1368683772161603e-13,
            5.6843418860808015e-14,
            2.8421709430404007e-14,
            1.4210854715202004e-14,
            7.105427357601002e-15,
            3.552713678800501e-15,
            1.7763568394002505e-15,
            8.881784197001252e-16,
            4.440892098500626e-16,
            2.220446049250313e-16,
            1.1102230246251565e-16,
            5.551115123125783e-17,
            2.7755575615628914e-17,
            1.3877787807814457e-17,
            6.938893903907228e-18,
            3.469446951953614e-18,
            1.734723475976807e-18,
            8.673617379884035e-19,
            4.336808689942018e-19,
            2.168404344971009e-19,
            1.0842021724855044e-19,
            5.421010862427522e-20,
        ];

        let m = self.num_registers as f64;

        // Raw harmonic mean estimate (optimized with lookup table)
        let mut sum = 0.0;
        let mut zeros = 0u32;
        for &r in &self.registers {
            sum += POW2_NEG[r as usize];
            if r == 0 {
                zeros += 1;
            }
        }

        let raw_estimate = self.alpha * m * m / sum;

        // Apply corrections for small and large cardinalities
        let estimate = if raw_estimate <= 2.5 * m {
            // Small range correction
            if zeros > 0 {
                m * (m / zeros as f64).ln()
            } else {
                raw_estimate
            }
        } else if raw_estimate > (1.0 / 30.0) * 2.0_f64.powi(64) {
            // Large range correction (for very large cardinalities)
            -2.0_f64.powi(64) * (1.0 - raw_estimate / 2.0_f64.powi(64)).ln()
        } else {
            raw_estimate
        };

        estimate.round() as u64
    }

    /// Merge another HyperLogLog into this one (union operation)
    ///
    /// After merging, this HLL represents the union of both sets.
    /// Both HLLs must have the same precision.
    ///
    /// **Optimized:** Uses SIMD-friendly max operation (compiler auto-vectorizes).
    ///
    /// # Arguments
    /// * `other` - Another HyperLogLog to merge
    ///
    /// # Panics
    /// Panics if precisions don't match
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(
            self.precision, other.precision,
            "Cannot merge HLLs with different precision"
        );

        // SIMD-friendly loop - compiler will auto-vectorize this
        // Using zip + max is cleaner and equally fast
        self.registers
            .iter_mut()
            .zip(other.registers.iter())
            .for_each(|(a, &b)| *a = (*a).max(b));
    }

    /// Get the precision parameter
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Get the number of registers
    pub fn num_registers(&self) -> usize {
        self.num_registers
    }

    /// Get the expected standard error rate
    pub fn expected_error(&self) -> f64 {
        1.04 / (self.num_registers as f64).sqrt()
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> usize {
        self.registers.len() + std::mem::size_of::<Self>()
    }

    /// Check if the HLL is empty (no elements inserted)
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }

    /// Clear all registers
    pub fn clear(&mut self) {
        self.registers.fill(0);
    }

    /// Serialize to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).expect("HyperLogLog serialization should not fail")
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::default_precision()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_hyperloglog() {
        let hll = HyperLogLog::new(12);
        assert_eq!(hll.precision(), 12);
        assert_eq!(hll.num_registers(), 4096);
        assert!(hll.is_empty());
    }

    #[test]
    fn test_insert_and_cardinality() {
        let mut hll = HyperLogLog::new(12);

        // Insert 1000 unique values
        for i in 0..1000 {
            hll.insert(&format!("user_{}", i));
        }

        let estimate = hll.cardinality();

        // Should be within ~5% of actual (1.6% standard error, 3 sigma = ~5%)
        assert!(
            (900..=1100).contains(&estimate),
            "Estimate {} should be close to 1000",
            estimate
        );
    }

    #[test]
    fn test_large_cardinality() {
        let mut hll = HyperLogLog::new(14); // Higher precision for large sets

        // Insert 100,000 unique values
        for i in 0..100_000 {
            hll.insert(&i);
        }

        let estimate = hll.cardinality();

        // Should be within ~3% of actual (0.8% standard error)
        let lower = 97_000;
        let upper = 103_000;
        assert!(
            estimate >= lower && estimate <= upper,
            "Estimate {} should be between {} and {}",
            estimate,
            lower,
            upper
        );
    }

    #[test]
    fn test_merge() {
        let mut hll1 = HyperLogLog::new(12);
        let mut hll2 = HyperLogLog::new(12);

        // hll1: users 0-999
        for i in 0..1000 {
            hll1.insert(&format!("user_{}", i));
        }

        // hll2: users 500-1499 (50% overlap)
        for i in 500..1500 {
            hll2.insert(&format!("user_{}", i));
        }

        // Merge
        hll1.merge(&hll2);

        // Should be ~1500 unique users
        let estimate = hll1.cardinality();
        assert!(
            (1400..=1600).contains(&estimate),
            "Merged estimate {} should be close to 1500",
            estimate
        );
    }

    #[test]
    fn test_duplicate_inserts() {
        let mut hll = HyperLogLog::new(12);

        // Insert same value 1000 times
        for _ in 0..1000 {
            hll.insert(&"same_value");
        }

        // Should estimate 1
        let estimate = hll.cardinality();
        assert!(
            estimate <= 2,
            "Estimate {} should be ~1 for duplicates",
            estimate
        );
    }

    #[test]
    fn test_serialization() {
        let mut hll = HyperLogLog::new(12);
        for i in 0..100 {
            hll.insert(&i);
        }

        let bytes = hll.to_bytes();
        let restored = HyperLogLog::from_bytes(&bytes).unwrap();

        assert_eq!(hll.precision(), restored.precision());
        assert_eq!(hll.cardinality(), restored.cardinality());
    }

    #[test]
    fn test_error_rate() {
        let hll = HyperLogLog::new(12);
        let error = hll.expected_error();

        // Precision 12: error ≈ 1.04 / sqrt(4096) ≈ 0.01625 (1.6%)
        assert!(
            error > 0.015 && error < 0.02,
            "Expected error {} should be ~1.6%",
            error
        );
    }

    #[test]
    fn test_clear() {
        let mut hll = HyperLogLog::new(12);
        hll.insert(&"test");
        assert!(!hll.is_empty());

        hll.clear();
        assert!(hll.is_empty());
        assert_eq!(hll.cardinality(), 0);
    }

    #[test]
    fn test_memory_usage() {
        let hll_12 = HyperLogLog::new(12);
        let hll_14 = HyperLogLog::new(14);

        // Precision 12: 4096 registers = ~4KB
        // Precision 14: 16384 registers = ~16KB
        assert!(hll_12.memory_bytes() < hll_14.memory_bytes());
        assert!(hll_12.memory_bytes() < 5000); // ~4KB
        assert!(hll_14.memory_bytes() < 20000); // ~16KB
    }

    #[test]
    #[should_panic(expected = "Precision must be between 4 and 18")]
    fn test_invalid_precision_low() {
        HyperLogLog::new(3);
    }

    #[test]
    #[should_panic(expected = "Precision must be between 4 and 18")]
    fn test_invalid_precision_high() {
        HyperLogLog::new(19);
    }

    #[test]
    #[should_panic(expected = "Cannot merge HLLs with different precision")]
    fn test_merge_different_precision() {
        let mut hll1 = HyperLogLog::new(12);
        let hll2 = HyperLogLog::new(14);
        hll1.merge(&hll2);
    }
}

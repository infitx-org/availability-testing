/**
 * Statistical Analysis Script for Pod Termination Impact Assessment
 *
 * This script analyzes the impact of pod terminations on system performance by examining
 * latency, throughput, and test failures in time series data from K6 load tests.
 *
 * USAGE:
 *   node statistical-analysis.js <folder-path> [clean-run-path]
 *
 * ARGUMENTS:
 *   folder-path      - Path to folder containing pod-terminations.csv and k6-time-series.csv
 *   clean-run-path   - (Optional) Path to clean run k6-time-series.csv for Method 0 baseline
 *
 * EXAMPLE:
 *   node statistical-analysis.js reports/core-services-run1
 *   node statistical-analysis.js reports/security-run1 reports/clean-run/k6-time-series.csv
 *
 * INPUT FILES:
 *   - pod-terminations.csv: Contains pod termination events (Pod, Termination Time, Status)
 *   - k6-time-series.csv: Contains performance metrics with columns:
 *     [0] Time, [1] VUs, [2] Latency, [3] Throughput, [4+] Check rates (for failure detection)
 *
 * OUTPUT:
 *   - statistical-analysis-report.csv: Generated in the supplied folder with columns:
 *     Pod, Termination Time, Status, Samples Before, Samples After,
 *     Before Latency Mean, After Latency Mean, Latency Change (%), Latency Significance,
 *     Before Throughput Mean, After Throughput Mean, Throughput Z-Score, Throughput Significance,
 *     Success Rate
 *
 * ANALYSIS METHODS:
 *   The script supports two baseline methods (configured via BASELINE_METHOD constant):
 *
 *   Method 0: Global Baseline
 *     - Uses a single baseline from either a clean run or the chaos run (omitting first/last 1min)
 *     - Compares 30s after each termination against this global baseline
 *     - Limitation: Doesn't account for performance drift over time (e.g., JVM warm-up)
 *
 *   Method 1: Local Baseline (RECOMMENDED)
 *     - Compares 30s before each termination with 30s after
 *     - Naturally handles performance drift by using local context
 *     - Best for detecting real disruptions caused by pod terminations
 *
 * SIGNIFICANCE THRESHOLDS (Percentage Change):
 *   - Highly Significant: > 10%
 *   - Significant:        > 5%
 *   - Marginal:           > 2%
 *   - Not Significant:    ≤ 2%
 *
 * SUCCESS RATE CALCULATION:
 *   - Calculates average success rate from check rate columns (indices 4+) in 30s after window
 *   - Check rate columns contain values between 0 and 1 representing success rate
 *   - Formula: (sum of all check rates / number of check rates) * 100
 *   - If no check rate data found, assumes 100% success
 *
 * PERFORMANCE DRIFT:
 *   During performance tests, latency often decreases over time due to:
 *   - Database connection pooling stabilization
 *   - Cache warming
 *   This can cause false positives/negatives with global baselines.
 *   Method 1 (Local Baseline) is recommended to handle this drift naturally.
 */

const fs = require('fs');
const path = require('path');

// ============================================
// CONFIGURATION
// ============================================
// Baseline method selection:
// 0 = Global baseline (clean run or omit first/last 1min from chaos run)
// 1 = Local baseline (30s before vs 30s after each termination)
const BASELINE_METHOD = 0;

const METHOD_NAMES = {
  0: 'Global Baseline',
  1: 'Local Baseline (Before vs After)'
};

/**
 * Parse CSV file and return array of objects
 */
function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.replace(/"/g, '').trim());

  return lines.slice(1).map(line => {
    const values = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    values.push(current.trim());

    const obj = {};
    headers.forEach((header, idx) => {
      obj[header] = values[idx];
    });
    return obj;
  });
}

/**
 * Convert timestamp to milliseconds
 */
function parseTimestamp(timestamp) {
  if (!isNaN(timestamp)) {
    const num = parseInt(timestamp);
    if (num > 1000000000000) {
      return num;
    }
    return num * 1000;
  }
  return new Date(timestamp).getTime();
}

/**
 * Calculate mean of an array
 */
function mean(values) {
  if (values.length === 0) return 0;
  return values.reduce((sum, val) => sum + val, 0) / values.length;
}

/**
 * Calculate standard deviation of an array
 */
function stdDev(values) {
  if (values.length === 0) return 0;
  const avg = mean(values);
  const squareDiffs = values.map(val => Math.pow(val - avg, 2));
  const avgSquareDiff = mean(squareDiffs);
  return Math.sqrt(avgSquareDiff);
}

/**
 * Find the baseline window by omitting first and last time duration (in seconds)
 * This gives a stable baseline without worrying about pod terminations
 */
function findBaselineWindowByTime(timeSeriesData, omitSeconds = 60) {
  console.log('\n--- Step 1: Finding Baseline Window ---');
  console.log(`Strategy: Omit first ${omitSeconds}s and last ${omitSeconds}s of data`);

  const totalSamples = timeSeriesData.length;
  console.log(`Total samples in dataset: ${totalSamples}`);

  if (totalSamples === 0) {
    console.error(`ERROR: No samples in dataset`);
    process.exit(1);
  }

  // Get timestamps
  const firstTimestamp = parseInt(timeSeriesData[0].Time);
  const lastTimestamp = parseInt(timeSeriesData[timeSeriesData.length - 1].Time);

  console.log(`Data range: ${new Date(firstTimestamp).toISOString()} to ${new Date(lastTimestamp).toISOString()}`);

  // Calculate baseline window by time
  const omitMillis = omitSeconds * 1000;
  const baselineStart = firstTimestamp + omitMillis;
  const baselineEnd = lastTimestamp - omitMillis;

  // Filter samples within the baseline window
  const baselineSamples = timeSeriesData.filter(row => {
    const t = parseInt(row.Time);
    return t >= baselineStart && t <= baselineEnd;
  });

  if (baselineSamples.length === 0) {
    console.error(`ERROR: No samples in baseline window. Dataset may be too short.`);
    process.exit(1);
  }

  console.log(`\nBaseline window:`);
  console.log(`  Start: ${new Date(baselineStart).toISOString()}`);
  console.log(`  End: ${new Date(baselineEnd).toISOString()}`);
  console.log(`  Total baseline samples: ${baselineSamples.length}`);
  console.log(`\n✓ Baseline window established`);

  return {
    start: baselineStart,
    end: baselineEnd,
    size: baselineSamples.length
  };
}

/**
 * Calculate baseline statistics for latency and throughput
 */
function calculateBaselineStats(timeSeriesData, baselineWindow, columnIndices) {
  console.log('\n--- Step 2: Calculating Baseline Statistics ---');

  const columns = Object.keys(timeSeriesData[0]);

  const baselineData = timeSeriesData.filter(row => {
    const t = parseInt(row.Time);
    return t >= baselineWindow.start && t <= baselineWindow.end;
  });

  console.log(`Extracting metrics from ${baselineData.length} baseline samples...`);

  const latencyValues = [];
  const throughputValues = [];

  baselineData.forEach(row => {
    const latency = row[columns[columnIndices.latency]];
    const throughput = row[columns[columnIndices.throughput]];

    const latencyVal = parseFloat(latency);
    const throughputVal = parseFloat(throughput);

    if (!isNaN(latencyVal) && latencyVal > 0) {
      latencyValues.push(latencyVal);
    }
    if (!isNaN(throughputVal) && throughputVal > 0) {
      throughputValues.push(throughputVal);
    }
  });

  const stats = {
    latency: {
      mean: mean(latencyValues),
      stdDev: stdDev(latencyValues),
      count: latencyValues.length
    },
    throughput: {
      mean: mean(throughputValues),
      stdDev: stdDev(throughputValues),
      count: throughputValues.length
    }
  };

  console.log(`Latency values: ${latencyValues.length} valid samples`);
  console.log(`  Mean: ${stats.latency.mean.toFixed(4)} ms`);
  console.log(`  StdDev: ${stats.latency.stdDev.toFixed(4)} ms`);
  console.log(`  Range: [${Math.min(...latencyValues).toFixed(2)}, ${Math.max(...latencyValues).toFixed(2)}] ms`);

  console.log(`Throughput values: ${throughputValues.length} valid samples`);
  console.log(`  Mean: ${stats.throughput.mean.toFixed(4)}`);
  console.log(`  StdDev: ${stats.throughput.stdDev.toFixed(4)}`);
  console.log(`  Range: [${Math.min(...throughputValues).toFixed(2)}, ${Math.max(...throughputValues).toFixed(2)}]`);

  return stats;
}

/**
 * Get metrics after a termination event
 * Returns samples within the specified time window (in seconds)
 */
function getMetricsAfterTermination(timeSeriesData, terminationTime, columnIndices, windowSeconds = 30) {
  const columns = Object.keys(timeSeriesData[0]);

  // Calculate window end time (terminationTime + windowSeconds)
  const windowEnd = terminationTime + (windowSeconds * 1000);

  // Get all samples within the time window after termination
  const afterData = timeSeriesData.filter(row => {
    const rowTime = parseInt(row.Time);
    return rowTime > terminationTime && rowTime <= windowEnd;
  });

  if (afterData.length === 0) {
    return null;
  }

  const latencyValues = [];
  const throughputValues = [];

  afterData.forEach(row => {
    const latency = row[columns[columnIndices.latency]];
    const throughput = row[columns[columnIndices.throughput]];

    const latencyVal = parseFloat(latency);
    const throughputVal = parseFloat(throughput);

    if (!isNaN(latencyVal) && latencyVal > 0) {
      latencyValues.push(latencyVal);
    }
    if (!isNaN(throughputVal) && throughputVal > 0) {
      throughputValues.push(throughputVal);
    }
  });

  // Calculate success rate from check rate columns
  // Check rate columns contain failure indicators based on PromQL: (-delta(k6_checks_rate[15s])) > 0
  // These columns (SDK_E2E_STATUS_COMPLETED, TRANSFERS__POST_TRANSFERS_RESPONSE_IS_200, etc.)
  // only appear when there are check failures
  // When values appear (> 0), they represent check rates during failure periods
  const checkRates = [];

  if (columnIndices.checkColumns && columnIndices.checkColumns.length > 0) {
    // Use the specific check columns found in the data
    for (const checkColIndex of columnIndices.checkColumns) {
      for (const row of afterData) {
        const value = parseFloat(row[columns[checkColIndex]]);
        if (!isNaN(value) && value >= 0 && value <= 1) {
          checkRates.push(value);
        }
      }
    }
  }

  // If check rates found in failure columns, calculate success rate
  // If no check rate data (empty), assume 100% success (no failures detected)
  let successRate = 100;
  if (checkRates.length > 0) {
    const avgCheckRate = mean(checkRates);
    // The check rate columns show failure rate, so success = 100 - failure rate
    successRate = 100 - (avgCheckRate * 100);
  }

  return {
    latency: {
      mean: mean(latencyValues),
      values: latencyValues,
      count: latencyValues.length
    },
    throughput: {
      mean: mean(throughputValues),
      values: throughputValues,
      count: throughputValues.length
    },
    windowSeconds: windowSeconds,
    successRate: successRate
  };
}

/**
 * Calculate percentage change
 * Returns the percentage change from baseline to current value
 */
function calculatePercentageChange(value, baselineMean) {
  if (baselineMean === 0) return 0;
  return ((value - baselineMean) / baselineMean) * 100;
}

/**
 * Calculate Z-score
 * Returns how many standard deviations away from the mean a value is
 */
function calculateZScore(value, baselineMean, baselineStdDev) {
  if (baselineStdDev === 0) return 0;
  return (value - baselineMean) / baselineStdDev;
}

/**
 * Assess significance based on percentage change
 * Using thresholds:
 * > 10% = Highly Significant
 * > 5% = Significant
 * > 2% = Marginal
 * <= 2% = Not Significant
 */
function assessSignificance(percentageChange) {
  const absChange = Math.abs(percentageChange);
  if (absChange > 10.0) {
    return 'Highly Significant';
  } else if (absChange > 5.0) {
    return 'Significant';
  } else if (absChange > 2.0) {
    return 'Marginal';
  } else {
    return 'Not Significant';
  }
}

/**
 * Assess significance based on Z-score
 * Using thresholds:
 * |Z| > 2.58 = Highly Significant (99% confidence, p < 0.01)
 * |Z| > 1.96 = Significant (95% confidence, p < 0.05)
 * |Z| > 1.28 = Marginal (90% confidence, p < 0.10)
 * |Z| <= 1.28 = Not Significant
 */
function assessSignificanceByZScore(zScore) {
  const absZ = Math.abs(zScore);
  if (absZ > 2.58) {
    return 'Highly Significant';
  } else if (absZ > 1.96) {
    return 'Significant';
  } else if (absZ > 1.28) {
    return 'Marginal';
  } else {
    return 'Not Significant';
  }
}

/**
 * Get metrics before a termination event (for local baseline)
 * Returns samples within the specified time window BEFORE termination
 */
function getMetricsBeforeTermination(timeSeriesData, terminationTime, columnIndices, windowSeconds = 60) {
  const columns = Object.keys(timeSeriesData[0]);

  // Calculate window start time (terminationTime - windowSeconds)
  const windowStart = terminationTime - (windowSeconds * 1000);

  // Get all samples within the time window before termination
  const beforeData = timeSeriesData.filter(row => {
    const rowTime = parseInt(row.Time);
    return rowTime >= windowStart && rowTime < terminationTime;
  });

  if (beforeData.length === 0) {
    return null;
  }

  const latencyValues = [];
  const throughputValues = [];

  beforeData.forEach(row => {
    const latency = row[columns[columnIndices.latency]];
    const throughput = row[columns[columnIndices.throughput]];

    const latencyVal = parseFloat(latency);
    const throughputVal = parseFloat(throughput);

    if (!isNaN(latencyVal) && latencyVal > 0) {
      latencyValues.push(latencyVal);
    }
    if (!isNaN(throughputVal) && throughputVal > 0) {
      throughputValues.push(throughputVal);
    }
  });

  return {
    latency: {
      mean: mean(latencyValues),
      stdDev: stdDev(latencyValues),
      values: latencyValues,
      count: latencyValues.length
    },
    throughput: {
      mean: mean(throughputValues),
      stdDev: stdDev(throughputValues),
      values: throughputValues,
      count: throughputValues.length
    },
    windowSeconds: windowSeconds
  };
}

// ============================================
// ANALYSIS METHODS
// ============================================

/**
 * Method 0: Global Baseline Analysis
 * Uses a single baseline (clean run or omit first/last period) for all comparisons
 */
function analyzeWithGlobalBaseline(timeSeriesData, podTerminations, columnIndices, baselineStats, windowSeconds = 30) {
  console.log('\n--- Method 0: Global Baseline Analysis ---');
  console.log(`Baseline: Mean Latency = ${baselineStats.latency.mean.toFixed(4)} ms, StdDev = ${baselineStats.latency.stdDev.toFixed(4)} ms`);
  console.log(`Baseline: Mean Throughput = ${baselineStats.throughput.mean.toFixed(4)}, StdDev = ${baselineStats.throughput.stdDev.toFixed(4)}`);
  console.log(`Will analyze ${windowSeconds}s time window after each pod termination\n`);

  const results = [];

  podTerminations.forEach((termination, index) => {
    const podName = termination.Pod;
    const terminationTime = parseTimestamp(termination['Termination Time']);
    const status = termination.Status;

    console.log(`[${index + 1}/${podTerminations.length}] Analyzing: ${podName}`);

    const metricsAfter = getMetricsAfterTermination(timeSeriesData, terminationTime, columnIndices, windowSeconds);

    if (!metricsAfter || metricsAfter.latency.count === 0) {
      console.log(`  ⚠️  No data found after termination`);
      results.push({
        Pod: podName,
        'Termination Time': termination['Termination Time'],
        Status: status,
        'Samples After': 0,
        'Baseline Latency Mean': baselineStats.latency.mean.toFixed(4),
        'Baseline Latency StdDev': baselineStats.latency.stdDev.toFixed(4),
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Z-Score': 'N/A',
        'Latency Significance': 'N/A',
        'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
        'Baseline Throughput StdDev': baselineStats.throughput.stdDev.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Z-Score': 'N/A',
        'Throughput Significance': 'N/A',
        'Success Rate': 'N/A'
      });
      return;
    }

    const latencyChange = calculatePercentageChange(metricsAfter.latency.mean, baselineStats.latency.mean);

    // Calculate Z-scores using baseline stats
    const latencyZScore = calculateZScore(metricsAfter.latency.mean, baselineStats.latency.mean, baselineStats.latency.stdDev);
    const throughputZScore = calculateZScore(metricsAfter.throughput.mean, baselineStats.throughput.mean, baselineStats.throughput.stdDev);

    const latencySignificance = assessSignificance(latencyChange);
    const throughputSignificance = assessSignificanceByZScore(throughputZScore);

    console.log(`  Latency: ${metricsAfter.latency.mean.toFixed(4)} ms (baseline: ${baselineStats.latency.mean.toFixed(4)} ms) → ${latencyChange > 0 ? '+' : ''}${latencyChange.toFixed(2)}% (Z=${latencyZScore.toFixed(2)}, ${latencySignificance})`);
    console.log(`  Throughput: ${metricsAfter.throughput.mean.toFixed(4)} (baseline: ${baselineStats.throughput.mean.toFixed(4)}) → Z=${throughputZScore.toFixed(2)}, ${throughputSignificance}`);
    console.log(`  Success Rate: ${metricsAfter.successRate.toFixed(2)}%`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples After': metricsAfter.latency.count,
      'Baseline Latency Mean': baselineStats.latency.mean.toFixed(4),
      'Baseline Latency StdDev': baselineStats.latency.stdDev.toFixed(4),
      'After Latency Mean': metricsAfter.latency.mean.toFixed(4),
      'Latency Change (%)': latencyChange.toFixed(2),
      'Latency Z-Score': latencyZScore.toFixed(2),
      'Latency Significance': latencySignificance,
      'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
      'Baseline Throughput StdDev': baselineStats.throughput.stdDev.toFixed(4),
      'After Throughput Mean': metricsAfter.throughput.mean.toFixed(4),
      'Throughput Z-Score': throughputZScore.toFixed(2),
      'Throughput Significance': throughputSignificance,
      'Success Rate': metricsAfter.successRate.toFixed(2) + '%'
    });
  });

  return results;
}

/**
 * Method 1: Local Baseline Analysis (Before vs After)
 * Compares 60s before termination with 30s after
 */
function analyzeWithLocalBaseline(timeSeriesData, podTerminations, columnIndices, beforeWindowSeconds = 60, afterWindowSeconds = 30) {
  console.log('\n--- Method 1: Local Baseline Analysis (Before vs After) ---');
  console.log(`Before window: ${beforeWindowSeconds}s, After window: ${afterWindowSeconds}s\n`);

  // Calculate baseline throughput stddev from entire dataset
  const columns = Object.keys(timeSeriesData[0]);
  const allThroughputValues = [];
  timeSeriesData.forEach(row => {
    const throughput = parseFloat(row[columns[columnIndices.throughput]]);
    if (!isNaN(throughput) && throughput > 0) {
      allThroughputValues.push(throughput);
    }
  });
  const baselineThroughputStdDev = stdDev(allThroughputValues);
  console.log(`Baseline throughput stddev (from entire dataset): ${baselineThroughputStdDev.toFixed(4)}\n`);

  const results = [];

  podTerminations.forEach((termination, index) => {
    const podName = termination.Pod;
    const terminationTime = parseTimestamp(termination['Termination Time']);
    const status = termination.Status;

    console.log(`[${index + 1}/${podTerminations.length}] Analyzing: ${podName}`);

    const metricsBefore = getMetricsBeforeTermination(timeSeriesData, terminationTime, columnIndices, beforeWindowSeconds);
    const metricsAfter = getMetricsAfterTermination(timeSeriesData, terminationTime, columnIndices, afterWindowSeconds);

    if (!metricsBefore || metricsBefore.latency.count === 0) {
      console.log(`  ⚠️  No data found before termination`);
      results.push({
        Pod: podName,
        'Termination Time': termination['Termination Time'],
        Status: status,
        'Samples Before': 0,
        'Samples After': metricsAfter ? metricsAfter.latency.count : 0,
        'Before Latency Mean': 'N/A',
        'Before Latency StdDev': 'N/A',
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Z-Score': 'N/A',
        'Latency Significance': 'N/A',
        'Before Throughput Mean': 'N/A',
        'Baseline Throughput StdDev': baselineThroughputStdDev.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Z-Score': 'N/A',
        'Throughput Significance': 'N/A',
        'Success Rate': metricsAfter ? metricsAfter.successRate.toFixed(2) + '%' : 'N/A'
      });
      return;
    }

    if (!metricsAfter || metricsAfter.latency.count === 0) {
      console.log(`  ⚠️  No data found after termination`);
      results.push({
        Pod: podName,
        'Termination Time': termination['Termination Time'],
        Status: status,
        'Samples Before': metricsBefore.latency.count,
        'Samples After': 0,
        'Before Latency Mean': metricsBefore.latency.mean.toFixed(4),
        'Before Latency StdDev': metricsBefore.latency.stdDev.toFixed(4),
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Z-Score': 'N/A',
        'Latency Significance': 'N/A',
        'Before Throughput Mean': metricsBefore.throughput.mean.toFixed(4),
        'Baseline Throughput StdDev': baselineThroughputStdDev.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Z-Score': 'N/A',
        'Throughput Significance': 'N/A',
        'Success Rate': 'N/A'
      });
      return;
    }

    // Calculate percentage change using local before baseline
    const latencyChange = calculatePercentageChange(metricsAfter.latency.mean, metricsBefore.latency.mean);

    // Calculate Z-scores
    // For latency: compare after mean with before mean and stddev (as before)
    const latencyZScore = calculateZScore(metricsAfter.latency.mean, metricsBefore.latency.mean, metricsBefore.latency.stdDev);

    // For throughput: use baseline stddev from entire dataset
    const throughputZScore = calculateZScore(metricsAfter.throughput.mean, metricsBefore.throughput.mean, baselineThroughputStdDev);

    const latencySignificance = assessSignificance(latencyChange);
    const throughputSignificance = assessSignificanceByZScore(throughputZScore);

    console.log(`  Before: Latency=${metricsBefore.latency.mean.toFixed(4)} ms (±${metricsBefore.latency.stdDev.toFixed(4)}), Throughput=${metricsBefore.throughput.mean.toFixed(4)} (±${metricsBefore.throughput.stdDev.toFixed(4)})`);
    console.log(`  After:  Latency=${metricsAfter.latency.mean.toFixed(4)} ms, Throughput=${metricsAfter.throughput.mean.toFixed(4)}`);
    console.log(`  Impact: Latency ${latencyChange > 0 ? '+' : ''}${latencyChange.toFixed(2)}% (Z=${latencyZScore.toFixed(2)}, ${latencySignificance}), Throughput Z=${throughputZScore.toFixed(2)} (${throughputSignificance})`);
    console.log(`  Success Rate: ${metricsAfter.successRate.toFixed(2)}%`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples Before': metricsBefore.latency.count,
      'Samples After': metricsAfter.latency.count,
      'Before Latency Mean': metricsBefore.latency.mean.toFixed(4),
      'Before Latency StdDev': metricsBefore.latency.stdDev.toFixed(4),
      'After Latency Mean': metricsAfter.latency.mean.toFixed(4),
      'Latency Change (%)': latencyChange.toFixed(2),
      'Latency Z-Score': latencyZScore.toFixed(2),
      'Latency Significance': latencySignificance,
      'Before Throughput Mean': metricsBefore.throughput.mean.toFixed(4),
      'Baseline Throughput StdDev': baselineThroughputStdDev.toFixed(4),
      'After Throughput Mean': metricsAfter.throughput.mean.toFixed(4),
      'Throughput Z-Score': throughputZScore.toFixed(2),
      'Throughput Significance': throughputSignificance,
      'Success Rate': metricsAfter.successRate.toFixed(2) + '%'
    });
  });

  return results;
}

/**
 * Generate statistical analysis report
 */
function generateReport() {
  // Get folder path from command line argument
  const folderPath = process.argv[2] || __dirname;
  const cleanRunPath = process.argv[3]; // Optional clean run file path

  const podTerminationsPath = path.join(folderPath, 'pod-terminations.csv');
  const timeSeriesPath = path.join(folderPath, 'k6-time-series.csv');

  // Load data
  const podTerminations = parseCSV(podTerminationsPath);
  const timeSeriesData = parseCSV(timeSeriesPath);

  console.log(`Loaded ${podTerminations.length} pod terminations`);
  console.log(`Loaded ${timeSeriesData.length} time series data points`);

  // Dynamically determine column indices by searching for column names
  const columns = Object.keys(timeSeriesData[0]);
  const latencyIndex = columns.findIndex(col => col.includes('Latency'));
  const throughputIndex = columns.findIndex(col => col.includes('Throughput'));

  // Find check columns for success rate calculation (these columns only appear when there are failures)
  const checkColumns = [
    'SDK_E2E_STATUS_COMPLETED',
    'TRANSFERS__POST_TRANSFERS_RESPONSE_IS_200',
    'TRANSFERS__PUT_TRANSFERS_ACCEPT_CONVERSION_RESPONSE_IS_200',
    'TRANSFERS__PUT_TRANSFERS_ACCEPT_PARTY_RESPONSE_IS_200',
    'TRANSFERS__PUT_TRANSFERS_ACCEPT_QUOTE_RESPONSE_IS_200'
  ];

  const checkColumnIndices = [];
  checkColumns.forEach(checkCol => {
    const idx = columns.findIndex(col => col === checkCol);
    if (idx !== -1) {
      checkColumnIndices.push(idx);
    }
  });

  if (latencyIndex === -1 || throughputIndex === -1) {
    console.error('Error: Could not find Latency or Throughput columns in the data');
    console.error('Available columns:', columns);
    process.exit(1);
  }

  const columnIndices = {
    latency: latencyIndex,
    throughput: throughputIndex,
    checkColumns: checkColumnIndices
  };

  console.log(`Using columns - Latency: ${columns[columnIndices.latency]} (index ${latencyIndex}), Throughput: ${columns[columnIndices.throughput]} (index ${throughputIndex})`);
  if (checkColumnIndices.length > 0) {
    console.log(`Found ${checkColumnIndices.length} check columns for success rate calculation:`, checkColumnIndices.map(i => columns[i]));
  }
  console.log(`\nAnalysis Method: ${BASELINE_METHOD} - ${METHOD_NAMES[BASELINE_METHOD]}`);

  // Route to appropriate analysis method
  let results;

  switch(BASELINE_METHOD) {
    case 0: {
      // Method 0: Global Baseline
      let baselineData;
      let baselineWindow;
      let baselineColumnIndices = columnIndices; // Use same indices by default

      if (cleanRunPath) {
        console.log(`Using clean run file for baseline: ${cleanRunPath}`);
        baselineData = parseCSV(cleanRunPath);
        console.log(`Loaded ${baselineData.length} samples from clean run`);
        baselineWindow = findBaselineWindowByTime(baselineData, 60);

        // Calculate column indices for the baseline data (may be different from chaos run)
        const baselineColumns = Object.keys(baselineData[0]);
        const baselineLatencyIndex = baselineColumns.findIndex(col => col.includes('Latency'));
        const baselineThroughputIndex = baselineColumns.findIndex(col => col.includes('Throughput'));

        // Find check columns in baseline data
        const baselineCheckColumnIndices = [];
        checkColumns.forEach(checkCol => {
          const idx = baselineColumns.findIndex(col => col === checkCol);
          if (idx !== -1) {
            baselineCheckColumnIndices.push(idx);
          }
        });

        baselineColumnIndices = {
          latency: baselineLatencyIndex,
          throughput: baselineThroughputIndex,
          checkColumns: baselineCheckColumnIndices
        };

        console.log(`Baseline columns - Latency: ${baselineColumns[baselineLatencyIndex]} (index ${baselineLatencyIndex}), Throughput: ${baselineColumns[baselineThroughputIndex]} (index ${baselineThroughputIndex})`);
      } else {
        console.log(`Using chaos run data for baseline (no clean run provided)`);
        baselineData = timeSeriesData;
        baselineWindow = findBaselineWindowByTime(timeSeriesData, 60);
      }

      const baselineStats = calculateBaselineStats(baselineData, baselineWindow, baselineColumnIndices);
      results = analyzeWithGlobalBaseline(timeSeriesData, podTerminations, columnIndices, baselineStats, 30);
      break;
    }

    case 1: {
      // Method 1: Local Baseline (before vs after)
      results = analyzeWithLocalBaseline(timeSeriesData, podTerminations, columnIndices, 30, 30);
      break;
    }

    default:
      console.error(`ERROR: Invalid BASELINE_METHOD (${BASELINE_METHOD}). Valid values are: 0, 1`);
      process.exit(1);
  }

  // Generate CSV output
  const outputPath = path.join(folderPath, 'statistical-analysis-report.csv');
  const headers = Object.keys(results[0]);
  const csvContent = [
    headers.join(','),
    ...results.map(row => headers.map(h => `"${row[h]}"`).join(','))
  ].join('\n');

  fs.writeFileSync(outputPath, csvContent);
  console.log(`\nReport generated: ${outputPath}`);

  // Print summary
  console.log('\n=== Statistical Significance Summary ===\n');
  results.forEach(result => {
    console.log(`Pod: ${result.Pod}`);
    console.log(`  Samples: ${result['Samples Analyzed']}`);
    console.log(`  Latency: ${result['After Latency Mean']} ms (Z=${result['Latency Z-Score']}) - ${result['Latency Significance']}`);
    console.log(`  Throughput: ${result['After Throughput Mean']} (Z=${result['Throughput Z-Score']}) - ${result['Throughput Significance']}`);
    console.log('');
  });

  // Summary statistics
  const significantLatency = results.filter(r => r['Latency Significance'] === 'Significant' || r['Latency Significance'] === 'Highly Significant').length;
  const significantThroughput = results.filter(r => r['Throughput Significance'] === 'Significant' || r['Throughput Significance'] === 'Highly Significant').length;

  console.log(`\n=== Overall Summary ===`);
  console.log(`Total pod terminations: ${results.length}`);
  console.log(`Statistically significant latency impacts: ${significantLatency}`);
  console.log(`Statistically significant throughput impacts: ${significantThroughput}`);
}

// Run the report
generateReport();

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
 *     Before Throughput Mean, After Throughput Mean, Throughput Change (%), Throughput Significance,
 *     Failures Detected
 *
 * ANALYSIS METHODS:
 *   The script supports three baseline methods (configured via BASELINE_METHOD constant):
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
 *   Method 2: Detrended Analysis
 *     - Removes linear trend from entire dataset using regression
 *     - Then compares 30s before vs 30s after each termination
 *     - Useful when there's systematic drift (latency decreasing, throughput increasing)
 *
 * SIGNIFICANCE THRESHOLDS (Percentage Change):
 *   - Highly Significant: > 10%
 *   - Significant:        > 5%
 *   - Marginal:           > 2%
 *   - Not Significant:    ≤ 2%
 *
 * FAILURE DETECTION:
 *   - Checks columns at index 4+ (check rate columns) in the 30s after window
 *   - If any value is between 0 and 1 (indicating failed checks), marks as "YES"
 *   - Otherwise marks as "NO"
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
// 2 = Detrended analysis (remove linear trend, then compare)
const BASELINE_METHOD = 1;

const METHOD_NAMES = {
  0: 'Global Baseline',
  1: 'Local Baseline (Before vs After)',
  2: 'Detrended Analysis (Linear Regression)'
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

  // Check for test failures in check rate columns (indices 4+)
  let hasFailures = false;
  for (let i = 4; i < columns.length; i++) {
    for (const row of afterData) {
      const value = parseFloat(row[columns[i]]);
      if (!isNaN(value) && value > 0 && value < 1) {
        hasFailures = true;
        break;
      }
    }
    if (hasFailures) break;
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
    hasFailures: hasFailures
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

/**
 * Calculate linear regression for trend analysis
 * Returns slope and intercept: y = slope * x + intercept
 */
function linearRegression(xValues, yValues) {
  const n = xValues.length;
  const sumX = xValues.reduce((a, b) => a + b, 0);
  const sumY = yValues.reduce((a, b) => a + b, 0);
  const sumXY = xValues.reduce((sum, x, i) => sum + x * yValues[i], 0);
  const sumXX = xValues.reduce((sum, x) => sum + x * x, 0);

  const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;

  return { slope, intercept };
}

/**
 * Detrend time series data by removing linear trend
 */
function detrendData(timeSeriesData, columnIndices) {
  const columns = Object.keys(timeSeriesData[0]);

  // Extract timestamps and values
  const timestamps = timeSeriesData.map(row => parseInt(row.Time));
  const latencyValues = [];
  const throughputValues = [];

  timeSeriesData.forEach(row => {
    const latency = parseFloat(row[columns[columnIndices.latency]]);
    const throughput = parseFloat(row[columns[columnIndices.throughput]]);

    latencyValues.push(!isNaN(latency) && latency > 0 ? latency : 0);
    throughputValues.push(!isNaN(throughput) && throughput > 0 ? throughput : 0);
  });

  // Calculate linear regression for latency
  const latencyRegression = linearRegression(timestamps, latencyValues);
  const throughputRegression = linearRegression(timestamps, throughputValues);

  // Calculate mean for centering
  const latencyMean = mean(latencyValues.filter(v => v > 0));
  const throughputMean = mean(throughputValues.filter(v => v > 0));

  // Create detrended data
  const detrendedData = timeSeriesData.map((row, index) => {
    const timestamp = timestamps[index];
    const latency = latencyValues[index];
    const throughput = throughputValues[index];

    // Detrend: remove trend and add back mean to center
    const detrendedLatency = latency > 0
      ? latency - (latencyRegression.slope * timestamp + latencyRegression.intercept) + latencyMean
      : latency;
    const detrendedThroughput = throughput > 0
      ? throughput - (throughputRegression.slope * timestamp + throughputRegression.intercept) + throughputMean
      : throughput;

    return {
      ...row,
      detrendedLatency,
      detrendedThroughput
    };
  });

  return {
    data: detrendedData,
    latencyRegression,
    throughputRegression,
    latencyMean,
    throughputMean
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
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Significance': 'N/A',
        'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Change (%)': 'N/A',
        'Throughput Significance': 'N/A',
        'Failures Detected': 'N/A'
      });
      return;
    }

    const latencyChange = calculatePercentageChange(metricsAfter.latency.mean, baselineStats.latency.mean);
    const throughputChange = calculatePercentageChange(metricsAfter.throughput.mean, baselineStats.throughput.mean);

    const latencySignificance = assessSignificance(latencyChange);
    const throughputSignificance = assessSignificance(throughputChange);

    console.log(`  Latency: ${metricsAfter.latency.mean.toFixed(4)} ms (baseline: ${baselineStats.latency.mean.toFixed(4)} ms) → ${latencyChange > 0 ? '+' : ''}${latencyChange.toFixed(2)}% (${latencySignificance})`);
    console.log(`  Throughput: ${metricsAfter.throughput.mean.toFixed(4)} (baseline: ${baselineStats.throughput.mean.toFixed(4)}) → ${throughputChange > 0 ? '+' : ''}${throughputChange.toFixed(2)}% (${throughputSignificance})`);
    console.log(`  Failures: ${metricsAfter.hasFailures ? 'YES' : 'NO'}`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples After': metricsAfter.latency.count,
      'Baseline Latency Mean': baselineStats.latency.mean.toFixed(4),
      'After Latency Mean': metricsAfter.latency.mean.toFixed(4),
      'Latency Change (%)': latencyChange.toFixed(2),
      'Latency Significance': latencySignificance,
      'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
      'After Throughput Mean': metricsAfter.throughput.mean.toFixed(4),
      'Throughput Change (%)': throughputChange.toFixed(2),
      'Throughput Significance': throughputSignificance,
      'Failures Detected': metricsAfter.hasFailures ? 'YES' : 'NO'
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
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Significance': 'N/A',
        'Before Throughput Mean': 'N/A',
        'After Throughput Mean': 'N/A',
        'Throughput Change (%)': 'N/A',
        'Throughput Significance': 'N/A',
        'Failures Detected': metricsAfter && metricsAfter.hasFailures ? 'YES' : 'N/A'
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
        'After Latency Mean': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Significance': 'N/A',
        'Before Throughput Mean': metricsBefore.throughput.mean.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Change (%)': 'N/A',
        'Throughput Significance': 'N/A',
        'Failures Detected': 'N/A'
      });
      return;
    }

    // Calculate percentage change using local before baseline
    const latencyChange = calculatePercentageChange(metricsAfter.latency.mean, metricsBefore.latency.mean);
    const throughputChange = calculatePercentageChange(metricsAfter.throughput.mean, metricsBefore.throughput.mean);

    const latencySignificance = assessSignificance(latencyChange);
    const throughputSignificance = assessSignificance(throughputChange);

    console.log(`  Before: Latency=${metricsBefore.latency.mean.toFixed(4)} ms (±${metricsBefore.latency.stdDev.toFixed(4)}), Throughput=${metricsBefore.throughput.mean.toFixed(4)}`);
    console.log(`  After:  Latency=${metricsAfter.latency.mean.toFixed(4)} ms, Throughput=${metricsAfter.throughput.mean.toFixed(4)}`);
    console.log(`  Impact: Latency ${latencyChange > 0 ? '+' : ''}${latencyChange.toFixed(2)}% (${latencySignificance}), Throughput ${throughputChange > 0 ? '+' : ''}${throughputChange.toFixed(2)}% (${throughputSignificance})`);
    console.log(`  Failures: ${metricsAfter.hasFailures ? 'YES' : 'NO'}`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples Before': metricsBefore.latency.count,
      'Samples After': metricsAfter.latency.count,
      'Before Latency Mean': metricsBefore.latency.mean.toFixed(4),
      'After Latency Mean': metricsAfter.latency.mean.toFixed(4),
      'Latency Change (%)': latencyChange.toFixed(2),
      'Latency Significance': latencySignificance,
      'Before Throughput Mean': metricsBefore.throughput.mean.toFixed(4),
      'After Throughput Mean': metricsAfter.throughput.mean.toFixed(4),
      'Throughput Change (%)': throughputChange.toFixed(2),
      'Throughput Significance': throughputSignificance,
      'Failures Detected': metricsAfter.hasFailures ? 'YES' : 'NO'
    });
  });

  return results;
}

/**
 * Method 2: Detrended Analysis
 * Removes linear trend then compares before vs after
 */
function analyzeWithDetrending(timeSeriesData, podTerminations, columnIndices, beforeWindowSeconds = 60, afterWindowSeconds = 30) {
  console.log('\n--- Method 2: Detrended Analysis (Linear Regression) ---');

  // Detrend the entire dataset
  console.log('Calculating linear trend...');
  const detrendResult = detrendData(timeSeriesData, columnIndices);

  console.log(`Latency trend: slope = ${detrendResult.latencyRegression.slope.toExponential(4)} ms/ms`);
  console.log(`Throughput trend: slope = ${detrendResult.throughputRegression.slope.toExponential(4)} /ms`);
  console.log(`Before window: ${beforeWindowSeconds}s, After window: ${afterWindowSeconds}s\n`);

  const results = [];

  podTerminations.forEach((termination, index) => {
    const podName = termination.Pod;
    const terminationTime = parseTimestamp(termination['Termination Time']);
    const status = termination.Status;

    console.log(`[${index + 1}/${podTerminations.length}] Analyzing: ${podName}`);

    // Get detrended metrics before and after
    const beforeWindowStart = terminationTime - (beforeWindowSeconds * 1000);
    const afterWindowEnd = terminationTime + (afterWindowSeconds * 1000);

    const beforeSamples = detrendResult.data.filter(row => {
      const t = parseInt(row.Time);
      return t >= beforeWindowStart && t < terminationTime;
    });

    const afterSamples = detrendResult.data.filter(row => {
      const t = parseInt(row.Time);
      return t > terminationTime && t <= afterWindowEnd;
    });

    if (beforeSamples.length === 0 || afterSamples.length === 0) {
      console.log(`  ⚠️  Insufficient data`);
      results.push({
        Pod: podName,
        'Termination Time': termination['Termination Time'],
        Status: status,
        'Samples Before': beforeSamples.length,
        'Samples After': afterSamples.length,
        'Before Detrended Latency': 'N/A',
        'After Detrended Latency': 'N/A',
        'Latency Change (%)': 'N/A',
        'Latency Significance': 'N/A',
        'Before Detrended Throughput': 'N/A',
        'After Detrended Throughput': 'N/A',
        'Throughput Change (%)': 'N/A',
        'Throughput Significance': 'N/A',
        'Failures Detected': 'N/A'
      });
      return;
    }

    const beforeLatencies = beforeSamples.map(s => s.detrendedLatency).filter(v => v > 0);
    const afterLatencies = afterSamples.map(s => s.detrendedLatency).filter(v => v > 0);
    const beforeThroughputs = beforeSamples.map(s => s.detrendedThroughput).filter(v => v > 0);
    const afterThroughputs = afterSamples.map(s => s.detrendedThroughput).filter(v => v > 0);

    // Check for failures in check rate columns (indices 4+)
    const columns = Object.keys(timeSeriesData[0]);
    let hasFailures = false;
    for (let i = 4; i < columns.length; i++) {
      for (const row of afterSamples) {
        const value = parseFloat(row[columns[i]]);
        if (!isNaN(value) && value > 0 && value < 1) {
          hasFailures = true;
          break;
        }
      }
      if (hasFailures) break;
    }

    const beforeLatencyMean = mean(beforeLatencies);
    const beforeLatencyStdDev = stdDev(beforeLatencies);
    const afterLatencyMean = mean(afterLatencies);

    const beforeThroughputMean = mean(beforeThroughputs);
    const beforeThroughputStdDev = stdDev(beforeThroughputs);
    const afterThroughputMean = mean(afterThroughputs);

    const latencyChange = calculatePercentageChange(afterLatencyMean, beforeLatencyMean);
    const throughputChange = calculatePercentageChange(afterThroughputMean, beforeThroughputMean);

    const latencySignificance = assessSignificance(latencyChange);
    const throughputSignificance = assessSignificance(throughputChange);

    console.log(`  Detrended Before: Lat=${beforeLatencyMean.toFixed(4)} ms, Tput=${beforeThroughputMean.toFixed(4)}`);
    console.log(`  Detrended After:  Lat=${afterLatencyMean.toFixed(4)} ms, Tput=${afterThroughputMean.toFixed(4)}`);
    console.log(`  Impact: Latency ${latencyChange > 0 ? '+' : ''}${latencyChange.toFixed(2)}% (${latencySignificance}), Throughput ${throughputChange > 0 ? '+' : ''}${throughputChange.toFixed(2)}% (${throughputSignificance})`);
    console.log(`  Failures: ${hasFailures ? 'YES' : 'NO'}`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples Before': beforeLatencies.length,
      'Samples After': afterLatencies.length,
      'Before Detrended Latency': beforeLatencyMean.toFixed(4),
      'After Detrended Latency': afterLatencyMean.toFixed(4),
      'Latency Change (%)': latencyChange.toFixed(2),
      'Latency Significance': latencySignificance,
      'Before Detrended Throughput': beforeThroughputMean.toFixed(4),
      'After Detrended Throughput': afterThroughputMean.toFixed(4),
      'Throughput Change (%)': throughputChange.toFixed(2),
      'Throughput Significance': throughputSignificance,
      'Failures Detected': hasFailures ? 'YES' : 'NO'
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

  // Determine column indices
  const columnIndices = {
    latency: 2,
    throughput: 3,
    failures: 4
  };

  const columns = Object.keys(timeSeriesData[0]);
  console.log(`Using columns - Latency: ${columns[columnIndices.latency]}, Throughput: ${columns[columnIndices.throughput]}`);
  console.log(`\nAnalysis Method: ${BASELINE_METHOD} - ${METHOD_NAMES[BASELINE_METHOD]}`);

  // Route to appropriate analysis method
  let results;

  switch(BASELINE_METHOD) {
    case 0: {
      // Method 0: Global Baseline
      let baselineData;
      let baselineWindow;

      if (cleanRunPath) {
        console.log(`Using clean run file for baseline: ${cleanRunPath}`);
        baselineData = parseCSV(cleanRunPath);
        console.log(`Loaded ${baselineData.length} samples from clean run`);
        baselineWindow = findBaselineWindowByTime(baselineData, 60);
      } else {
        console.log(`Using chaos run data for baseline (no clean run provided)`);
        baselineData = timeSeriesData;
        baselineWindow = findBaselineWindowByTime(timeSeriesData, 60);
      }

      const baselineStats = calculateBaselineStats(baselineData, baselineWindow, columnIndices);
      results = analyzeWithGlobalBaseline(timeSeriesData, podTerminations, columnIndices, baselineStats, 30);
      break;
    }

    case 1: {
      // Method 1: Local Baseline (before vs after)
      results = analyzeWithLocalBaseline(timeSeriesData, podTerminations, columnIndices, 30, 30);
      break;
    }

    case 2: {
      // Method 2: Detrended Analysis
      results = analyzeWithDetrending(timeSeriesData, podTerminations, columnIndices, 30, 30);
      break;
    }

    default:
      console.error(`ERROR: Invalid BASELINE_METHOD (${BASELINE_METHOD}). Valid values are: 0, 1, 2`);
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

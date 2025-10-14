const fs = require('fs');
const path = require('path');

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
 * Find the maximum continuous window without any pod terminations
 * This window will be used as the baseline
 */
function findBaselineWindow(timeSeriesData, podTerminations) {
  console.log('\n--- Step 1: Finding Baseline Window ---');

  const terminationTimes = podTerminations.map(t => parseTimestamp(t['Termination Time'])).sort((a, b) => a - b);
  const firstTermination = terminationTimes[0];
  const lastTermination = terminationTimes[terminationTimes.length - 1];

  console.log(`First pod termination: ${new Date(firstTermination).toISOString()}`);
  console.log(`Last pod termination: ${new Date(lastTermination).toISOString()}`);

  // Get all time series timestamps
  const timestamps = timeSeriesData.map(row => parseInt(row.Time)).sort((a, b) => a - b);
  const firstTimestamp = timestamps[0];
  const lastTimestamp = timestamps[timestamps.length - 1];

  console.log(`Data range: ${new Date(firstTimestamp).toISOString()} to ${new Date(lastTimestamp).toISOString()}`);

  // Find the longest window without terminations
  let maxWindowStart = null;
  let maxWindowEnd = null;
  let maxWindowSize = 0;
  let maxWindowLocation = '';

  // Check window before first termination
  const beforeWindow = timeSeriesData.filter(row => parseInt(row.Time) < firstTermination);
  console.log(`\nChecking window BEFORE first termination: ${beforeWindow.length} samples`);
  if (beforeWindow.length > maxWindowSize) {
    maxWindowSize = beforeWindow.length;
    maxWindowStart = firstTimestamp;
    maxWindowEnd = firstTermination - 1;
    maxWindowLocation = 'before first termination';
  }

  // Check window after last termination
  const afterWindow = timeSeriesData.filter(row => parseInt(row.Time) > lastTermination);
  console.log(`Checking window AFTER last termination: ${afterWindow.length} samples`);
  if (afterWindow.length > maxWindowSize) {
    maxWindowSize = afterWindow.length;
    maxWindowStart = lastTermination + 1;
    maxWindowEnd = lastTimestamp;
    maxWindowLocation = 'after last termination';
  }

  // Check gaps between terminations
  console.log(`Checking gaps BETWEEN terminations...`);
  for (let i = 0; i < terminationTimes.length - 1; i++) {
    const gapStart = terminationTimes[i];
    const gapEnd = terminationTimes[i + 1];
    const gapWindow = timeSeriesData.filter(row => {
      const t = parseInt(row.Time);
      return t > gapStart && t < gapEnd;
    });
    console.log(`  Gap ${i + 1}: ${gapWindow.length} samples between ${new Date(gapStart).toISOString()} and ${new Date(gapEnd).toISOString()}`);
    if (gapWindow.length > maxWindowSize) {
      maxWindowSize = gapWindow.length;
      maxWindowStart = gapStart + 1;
      maxWindowEnd = gapEnd - 1;
      maxWindowLocation = `gap ${i + 1} between terminations`;
    }
  }

  console.log(`\n✓ Selected baseline window: ${maxWindowLocation} (${maxWindowSize} samples)`);

  return {
    start: maxWindowStart,
    end: maxWindowEnd,
    size: maxWindowSize
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
 * Returns samples within the specified window
 */
function getMetricsAfterTermination(timeSeriesData, terminationTime, columnIndices, sampleCount = 5) {
  const columns = Object.keys(timeSeriesData[0]);

  // Get the next N samples after termination
  const afterData = timeSeriesData
    .filter(row => parseInt(row.Time) > terminationTime)
    .slice(0, sampleCount);

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
    }
  };
}

/**
 * Calculate Z-score for statistical significance
 * Returns the number of standard deviations from baseline mean
 */
function calculateZScore(value, baselineMean, baselineStdDev) {
  if (baselineStdDev === 0) return 0;
  return (value - baselineMean) / baselineStdDev;
}

/**
 * Determine if change is statistically significant
 * Using Z-score thresholds:
 * |Z| > 2.0 = statistically significant (95% confidence)
 * |Z| > 3.0 = highly significant (99.7% confidence)
 */
function assessSignificance(zScore) {
  const absZ = Math.abs(zScore);
  if (absZ > 3.0) {
    return 'Highly Significant';
  } else if (absZ > 2.0) {
    return 'Significant';
  } else if (absZ > 1.0) {
    return 'Marginal';
  } else {
    return 'Not Significant';
  }
}

/**
 * Generate statistical analysis report
 */
function generateReport() {
  // Get folder path from command line argument
  const folderPath = process.argv[2] || __dirname;
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

  // Step 1: Find baseline window without any pod terminations
  const baselineWindow = findBaselineWindow(timeSeriesData, podTerminations);
  console.log(`\nBaseline Window (no pod terminations):`);
  console.log(`  Start: ${baselineWindow.start} (${new Date(baselineWindow.start).toISOString()})`);
  console.log(`  End: ${baselineWindow.end} (${new Date(baselineWindow.end).toISOString()})`);
  console.log(`  Sample Count: ${baselineWindow.size}`);

  // Step 2: Calculate baseline statistics
  const baselineStats = calculateBaselineStats(timeSeriesData, baselineWindow, columnIndices);
  console.log(`\nBaseline Statistics:`);
  console.log(`  Latency: Mean = ${baselineStats.latency.mean.toFixed(4)} ms, StdDev = ${baselineStats.latency.stdDev.toFixed(4)} ms`);
  console.log(`  Throughput: Mean = ${baselineStats.throughput.mean.toFixed(4)}, StdDev = ${baselineStats.throughput.stdDev.toFixed(4)}`);

  // Step 3 & 4: Analyze each pod termination for statistical significance
  console.log('\n--- Step 3 & 4: Analyzing Pod Terminations ---');

  const results = [];
  const sampleCount = 5; // Number of samples after kill to analyze

  console.log(`Will analyze ${sampleCount} samples after each pod termination`);
  console.log(`Total pod terminations to analyze: ${podTerminations.length}\n`);

  podTerminations.forEach((termination, index) => {
    const podName = termination.Pod;
    const terminationTime = parseTimestamp(termination['Termination Time']);
    const status = termination.Status;

    console.log(`[${index + 1}/${podTerminations.length}] Analyzing: ${podName}`);
    console.log(`  Termination time: ${new Date(terminationTime).toISOString()}`);

    const metricsAfter = getMetricsAfterTermination(timeSeriesData, terminationTime, columnIndices, sampleCount);

    if (!metricsAfter || metricsAfter.latency.count === 0) {
      console.log(`  ⚠️  No data found after termination`);
      results.push({
        Pod: podName,
        'Termination Time': termination['Termination Time'],
        Status: status,
        'Samples Analyzed': 0,
        'Baseline Latency Mean': baselineStats.latency.mean.toFixed(4),
        'Baseline Latency StdDev': baselineStats.latency.stdDev.toFixed(4),
        'After Latency Mean': 'N/A',
        'Latency Z-Score': 'N/A',
        'Latency Significance': 'N/A',
        'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
        'Baseline Throughput StdDev': baselineStats.throughput.stdDev.toFixed(4),
        'After Throughput Mean': 'N/A',
        'Throughput Z-Score': 'N/A',
        'Throughput Significance': 'N/A'
      });
      return;
    }

    console.log(`  Samples collected: ${metricsAfter.latency.count}`);

    // Calculate Z-scores
    const latencyZScore = calculateZScore(
      metricsAfter.latency.mean,
      baselineStats.latency.mean,
      baselineStats.latency.stdDev
    );

    const throughputZScore = calculateZScore(
      metricsAfter.throughput.mean,
      baselineStats.throughput.mean,
      baselineStats.throughput.stdDev
    );

    // Assess significance
    const latencySignificance = assessSignificance(latencyZScore);
    const throughputSignificance = assessSignificance(throughputZScore);

    console.log(`  Latency: ${metricsAfter.latency.mean.toFixed(4)} ms (baseline: ${baselineStats.latency.mean.toFixed(4)} ms)`);
    console.log(`    Z-Score: ${latencyZScore.toFixed(2)} → ${latencySignificance}`);
    console.log(`  Throughput: ${metricsAfter.throughput.mean.toFixed(4)} (baseline: ${baselineStats.throughput.mean.toFixed(4)})`);
    console.log(`    Z-Score: ${throughputZScore.toFixed(2)} → ${throughputSignificance}`);

    results.push({
      Pod: podName,
      'Termination Time': termination['Termination Time'],
      Status: status,
      'Samples Analyzed': metricsAfter.latency.count,
      'Baseline Latency Mean': baselineStats.latency.mean.toFixed(4),
      'Baseline Latency StdDev': baselineStats.latency.stdDev.toFixed(4),
      'After Latency Mean': metricsAfter.latency.mean.toFixed(4),
      'Latency Z-Score': latencyZScore.toFixed(2),
      'Latency Significance': latencySignificance,
      'Baseline Throughput Mean': baselineStats.throughput.mean.toFixed(4),
      'Baseline Throughput StdDev': baselineStats.throughput.stdDev.toFixed(4),
      'After Throughput Mean': metricsAfter.throughput.mean.toFixed(4),
      'Throughput Z-Score': throughputZScore.toFixed(2),
      'Throughput Significance': throughputSignificance
    });
  });

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

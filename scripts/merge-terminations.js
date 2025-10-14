const fs = require('fs');
const path = require('path');

/**
 * Parse CSV file and return array of objects
 */
function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.trim().split('\n');
  // Remove all quotes from headers
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
 * Convert timestamp to milliseconds (handles both formats)
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
 * Merge pod terminations into k6 time series data
 * @param {string} dataFolder - Folder containing the CSV files (relative or absolute)
 */
function mergeData(dataFolder = __dirname) {
  const resolvedFolder = path.resolve(process.cwd(), dataFolder);
  const podTerminationsPath = path.join(resolvedFolder, 'pod-terminations.csv');
  const timeSeriesPath = path.join(resolvedFolder, 'k6-time-series.csv');

  // Load data
  const podTerminations = parseCSV(podTerminationsPath);
  const timeSeriesData = parseCSV(timeSeriesPath);

  console.log(`Loaded ${podTerminations.length} pod terminations`);
  console.log(`Loaded ${timeSeriesData.length} time series data points`);

  // Get headers from k6 time series
  const content = fs.readFileSync(timeSeriesPath, 'utf-8');
  const headers = content.split('\n')[0].split(',').map(h => h.replace(/"/g, '').trim());

  // Add new "Pod Termination" header at the end
  const newHeaders = [...headers, 'Pod Termination'];

  console.log(`\nOriginal headers: ${headers.join(', ')}`);
  console.log(`New headers: ${newHeaders.join(', ')}`);

  // Convert all data to a unified format with timestamps
  const allData = [];

  // Add k6 time series data
  timeSeriesData.forEach(row => {
    // Add empty pod termination field
    row['Pod Termination'] = '';

    allData.push({
      timestamp: parseInt(row[headers[0]]), // Time column
      type: 'metric',
      data: row
    });
  });

  // Add pod terminations
  podTerminations.forEach(termination => {
    const timestamp = parseTimestamp(termination['Termination Time']);
    const podName = termination.Pod;

    // Create a row with timestamp and pod termination info, rest empty
    const terminationRow = {};
    newHeaders.forEach((header, idx) => {
      if (idx === 0) {
        // First column: keep timestamp
        terminationRow[header] = timestamp;
      } else if (header === 'Pod Termination') {
        // Last column: show pod termination
        terminationRow[header] = `${podName} killed`;
      } else {
        // All other columns: empty
        terminationRow[header] = '';
      }
    });

    allData.push({
      timestamp: timestamp,
      type: 'termination',
      data: terminationRow
    });
  });

  // Sort by timestamp
  allData.sort((a, b) => a.timestamp - b.timestamp);

  console.log(`\nMerged ${allData.length} total entries`);
  console.log(`  - ${timeSeriesData.length} metric entries`);
  console.log(`  - ${podTerminations.length} termination entries`);

  // Generate CSV output
  const outputPath = path.join(resolvedFolder, 'merged-time-series.csv');

  // Build CSV content with new headers
  const csvLines = [newHeaders.join(',')];

  allData.forEach(entry => {
    const row = entry.data;
    const values = newHeaders.map(header => {
      const value = row[header] || '';
      // Quote values that contain commas or special characters
      if (value.toString().includes(',') || value.toString().includes('"') || value.toString().includes('%')) {
        return `"${value.toString().replace(/"/g, '""')}"`;
      }
      return value;
    });
    csvLines.push(values.join(','));
  });

  fs.writeFileSync(outputPath, csvLines.join('\n'));
  console.log(`\nMerged data saved to: ${outputPath}`);

  // Show sample output
  console.log('\nSample merged data (first 15 entries):');
  allData.slice(0, 15).forEach(entry => {
    const timestamp = entry.data[newHeaders[0]];
    const podTermination = entry.data['Pod Termination'];
    if (entry.type === 'termination') {
      console.log(`  [TERMINATION] Time: ${timestamp}, Event: ${podTermination}`);
    } else {
      console.log(`  [METRIC] Time: ${timestamp}`);
    }
  });
}

// Run the merge
const dataFolder = process.argv[2] || '.';
mergeData(dataFolder);

const fs = require('fs');
const path = require('path');
require('dotenv').config();

// Configuration
const GRAFANA_URL = `${process.env.GRAFANA_URL}/api/annotations`;
const GRAFANA_TOKEN = process.env.GRAFANA_TOKEN;
const ANNOTATION_TAG = 'custom-annotation';

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
 * Create Grafana annotation
 */
async function createAnnotation(time, text, tags) {
  const payload = {
    time: parseInt(time),
    tags: tags,
    text: text
  };

  try {
    const response = await fetch(GRAFANA_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${GRAFANA_TOKEN}`
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }

    const result = await response.json();
    return { success: true, result };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

/**
 * Main function to process pod terminations and create annotations
 */
async function main() {
  const podTerminationsPath = path.join(__dirname, 'pod-terminations.csv');

  // Check if file exists
  if (!fs.existsSync(podTerminationsPath)) {
    console.error(`Error: File not found: ${podTerminationsPath}`);
    process.exit(1);
  }

  // Load pod terminations
  const podTerminations = parseCSV(podTerminationsPath);
  console.log(`Loaded ${podTerminations.length} pod terminations from CSV`);

  // Create annotations for each pod termination
  let successCount = 0;
  let failureCount = 0;

  for (let i = 0; i < podTerminations.length; i++) {
    const termination = podTerminations[i];
    const podName = termination.Pod;
    const terminationTime = termination['Termination Time'];

    console.log(`\nProcessing (${i + 1}/${podTerminations.length}): ${podName}`);

    const result = await createAnnotation(
      terminationTime,
      podName,
      [ANNOTATION_TAG]
    );

    if (result.success) {
      console.log(`✓ Successfully created annotation for ${podName}`);
      successCount++;
    } else {
      console.error(`✗ Failed to create annotation for ${podName}: ${result.error}`);
      failureCount++;
    }

    // Small delay to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Summary:`);
  console.log(`  Total: ${podTerminations.length}`);
  console.log(`  Success: ${successCount}`);
  console.log(`  Failed: ${failureCount}`);
  console.log(`${'='.repeat(60)}`);
}

// Run the script
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

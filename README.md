# Availability Testing

This repository contains useful scripts and tools to support availability testing in Kubernetes environments, including analyzing pod terminations, creating Grafana annotations, and merging termination events with performance metrics.

## Scripts

### 1. Create Grafana Annotations

Creates Grafana annotations for each pod termination event from a CSV file. This allows you to visualize pod terminations directly on your Grafana dashboards.

**Usage:**
```bash
node scripts/create-grafana-annotations.js <path-to-csv>
```

**Example:**
```bash
node scripts/create-grafana-annotations.js ./reports/istio-run1/pod-terminations.csv
```

**Input CSV Format:**
The CSV file should contain at minimum:
- `Pod`: Pod name
- `Termination Time`: Unix timestamp in milliseconds

**Features:**
- Parses CSV files with pod termination data
- Creates timestamped annotations in Grafana via API
- Includes rate limiting (100ms delay between requests)
- Provides summary with success/failure counts

**Environment Variables:**
- `GRAFANA_URL`: Your Grafana instance URL
- `GRAFANA_TOKEN`: API token with annotation write permissions
- `PREFIX`: Custom prefix for annotation text (optional, default: "Pod")

Create a `.env` file in the project root with the above variables.


### 2. Merge Terminations with Time Series

Merges pod termination events with k6 performance metrics into a single CSV file for unified analysis and visualization.

**Usage:**
```bash
node scripts/merge-terminations.js <path-to-report-folder>
```

**Example:**
```bash
node scripts/merge-terminations.js ./reports/istio-run1
```

**Required Input Files:**
The specified folder must contain:
- `pod-terminations.csv`: Pod termination events with timestamp and pod name
- `k6-time-series.csv`: k6 performance metrics time series data

**Output:**
- `merged-time-series.csv`: Combined CSV with all metrics and a new "Pod Termination" column
- Entries are sorted chronologically by timestamp
- Termination events appear as rows with only timestamp and pod termination info
- Metric rows retain all original k6 data with empty pod termination field

**Features:**
- Handles both Unix timestamps (seconds/milliseconds) and ISO date formats
- Preserves all original metric columns
- Adds "Pod Termination" column to indicate termination events
- Sorts all data chronologically
- Displays sample merged data in console output

## Typical Workflow

1. Run your HA tests and collect pod termination data and k6 metrics
2. Merge the data for local analysis:
   ```bash
   node scripts/merge-terminations.js ./reports/your-test-run
   ```
3. Create Grafana annotations for visualization:
   ```bash
   node scripts/create-grafana-annotations.js ./reports/your-test-run/pod-terminations.csv
   ```
4. Produce the files as evidence of your availability testing.

## Data Format Examples

### pod-terminations.csv
```csv
Pod,Termination Time
service-pod-abc123,1704067200000
api-pod-def456,1704067260000
```

### k6-time-series.csv
```csv
Time,Success Rate,Error Rate,Response Time p95
1704067200000,99.5,0.5,123.45
1704067260000,97.2,2.8,456.78
```

### merged-time-series.csv (output)
```csv
Time,Success Rate,Error Rate,Response Time p95,Pod Termination
1704067200000,99.5,0.5,123.45,
1704067200000,,,,service-pod-abc123 killed
1704067260000,97.2,2.8,456.78,
1704067260000,,,,api-pod-def456 killed
```

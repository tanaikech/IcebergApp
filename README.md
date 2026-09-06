# IcebergApp

<a name="top"></a>
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

<p align="center">
  <b>Turn Google Sheets into a Petabyte Lakehouse with Sub-Second ACID Queries.</b><br>
  <sub>A Google Apps Script library for managing Google Cloud Lakehouse for Apache Iceberg.</sub>
</p>

---

## Overview

**IcebergApp** is a high-performance Google Apps Script (GAS) library designed to manage and orchestrate [Apache Iceberg](https://iceberg.apache.org/) tables backed by Google Cloud Lakehouse (BigQuery and Google Cloud Storage).

By decoupling storage from compute, Google Cloud's Lakehouse architecture enables serverless, unified data lakehouse management. **IcebergApp** bridges this enterprise analytical ecosystem with Google Workspace:

- **Serverless Lakehouse Orchestration**: Manage Iceberg tables, schemas, and partition policies directly from Google Apps Script.
- **ACID Transactions & Predicate Pushdown**: Execute high-throughput DDL, DML (`INSERT`, `UPDATE`, `DELETE`), and filter queries that leverage Iceberg's metadata pruning.
- **Workspace Interoperability**: Seamless bidirectional data pipelines between Google Sheets (2D arrays) and petabyte-scale Iceberg tables without operational latency.
- **Overcoming Google Sheets Limits & CRUD Latency**: Permanently bypasses Google Sheets' 10-million cell limit and eliminates crippling latency in row searching, appending, in-place updating, and `deleteRow()` operations by offloading massive data mutations to serverless Parquet storage.
- **Time Travel Queries**: Query historical snapshots using Iceberg's native snapshot isolation.
- **Democratizing Apache Iceberg**: Eliminates the need for Java, Scala, Python (PyIceberg), or dedicated Spark clusters—enabling Workspace developers to orchestrate enterprise lakehouses using pure, fluent JavaScript.
- **Open Multi-Engine Federation (Zero Vendor Lock-In)**: Data committed to Cloud Storage resides as open Apache Parquet files, concurrently accessible by Apache Spark, Trino, Snowflake, and BigQuery.

---

## Architecture & Mechanics

![IcebergApp Architecture & Workflow](images/workflow_architecture_infographic.jpg)

IcebergApp uses the **BigQuery Advanced Service** as an accelerated query engine that interacts with the **Lakehouse Runtime Catalog (Iceberg REST Catalog)**. Metadata-level pruning (Min/Max statistics and partition manifest checks) occurs prior to scanning actual Parquet data files on Cloud Storage, significantly reducing scanned bytes and execution time within Google Apps Script runtime constraints.

```
[ Google Apps Script (IcebergApp) ]
                │
                ▼ (DML / DDL via BigQuery API)
[ BigQuery Lakehouse Engine / REST Catalog ]
                │
        (Metadata Pruning)
                │
                ▼
[ Cloud Storage (gs://bucket/) - Iceberg Parquet & Avro Metadata ]
```

---

## Overcoming Google Sheets Limitations: Why IcebergApp?

Google Sheets is the most intuitive and widely used frontline data interface. However, when used as an analytical data store or high-volume operational backend, developers invariably run into severe architectural limits:

| Operation | Google Sheets / GAS Bottlenecks | IcebergApp Solution |
| :--- | :--- | :--- |
| **Capacity** | **10-Million Cell Ceiling**: File bloat, slow loads, quota failures. | **Infinite Petabyte Lakehouse**: Parquet data stored on durable Cloud Storage. |
| **Search / Query** | **V8 Memory Overload & 6-Min Timeout**: Pulling massive ranges via `getValues()` to loop in V8 exhausts RAM and triggers timeouts. | **Predicate Pushdown (1–2s)**: Metadata Min/Max evaluation skips irrelevant files; returns only filtered rows. |
| **Insert / Append** | **Formula Recalculation Freeze**: Batch `appendRow()` / `setValues()` forces full workbook formula and revision recalculations. | **Atomic Parquet Streaming**: Direct micro-batch streaming via `table.insertValues()` with $O(1)$ client cost. |
| **Update (In-Place)**| **Full-Grid Rewrites**: Searching rows in memory and overwriting ranges causes severe RPC latency. | **Distributed SQL DML**: In-place `table.update(...)` executed directly on storage without cell-by-cell iteration. |
| **Delete (Rows)** | **Fatal `deleteRow()` Loop**: Deleting rows shifts the entire grid upward per call, freezing scripts or timing out. | **Metadata-Level Atomic Delete**: `table.deleteRows(...)` purges records instantaneously via metadata commits. |

By maintaining Google Sheets as an agile, lightweight presentation surface and delegating storage and heavy compute to Apache Iceberg and BigQuery, IcebergApp completely eliminates these performance barriers.

---

## Setup & Requirements

To use this library, you must link your Google Apps Script project to a standard Google Cloud Platform (GCP) project and enable the required APIs.

### 1. Create or Configure GCP Project

1. Open the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing project (e.g., `your-gcp-project-id`).
3. Note your **Project Number** and **Project ID** from the Project Dashboard.
4. Enable the following APIs in **APIs & Services > Library**:
   - **BigQuery API**
   - **Google Cloud Storage JSON API**

### 2. Link GCP Project to Google Apps Script

1. Open your Google Apps Script project.
2. Click **Project Settings** (gear icon) in the left sidebar.
3. Under **Google Cloud Platform (GCP) Project**, click **Change project**.
4. Enter your **GCP Project Number** and confirm.

### 3. Enable BigQuery Advanced Service

1. In the Apps Script editor, click the **+** icon next to **Services**.
2. Select **BigQuery API**.
3. Choose version `v2` and keep the identifier as `BigQuery`.
4. Click **Add**.

### 4. Configure Manifest (`appsscript.json`)

Enable the manifest editor by checking **Show "appsscript.json" manifest file in editor** under **Project Settings**. Update your `appsscript.json` with the required configuration.

#### Standard Consumer Profile
```json
{
  "timeZone": "Asia/Tokyo",
  "dependencies": {
    "enabledAdvancedServices": [
      {
        "userSymbol": "BigQuery",
        "serviceId": "bigquery",
        "version": "v2"
      }
    ]
  },
  "exceptionLogging": "STACKDRIVER",
  "runtimeVersion": "V8",
  "oauthScopes": [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets"
  ]
}
```

#### Test Suite Runner Profile (`src/test.js`)
When running the automated lifecycle test suite, Cloud Storage and Drive permissions are required for autonomous provisioning and zero-residue cleanup:
```json
{
  "timeZone": "Asia/Tokyo",
  "dependencies": {
    "enabledAdvancedServices": [
      {
        "userSymbol": "BigQuery",
        "serviceId": "bigquery",
        "version": "v2"
      }
    ]
  },
  "exceptionLogging": "STACKDRIVER",
  "runtimeVersion": "V8",
  "oauthScopes": [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/script.external_request"
  ]
}
```

---

## Installation

You can install IcebergApp using either of the following methods:

1. **Direct Inclusion**: Copy the contents of `src/IcebergApp.js` directly into your Google Apps Script project.
2. **As a Library**: Add the script ID as a library with the identifier `IcebergApp`.

> [!NOTE]
> Both `IcebergApp.openByCatalog(...)` and global `openByCatalog(...)` are fully supported in all environments.

---

## Methods

### `IcebergApp`

| Method | Return | Description |
| :--- | :--- | :--- |
| `openByCatalog(projectId, catalogName, location)` | `IcebergApp` | Initializes the IcebergApp instance for a specific catalog/dataset. |
| `create(tableName, options)` | `IcebergTable` | Creates a new Apache Iceberg / BigLake table. |
| `getTables()` | `Array<IcebergTable>` | Lists all tables within the catalog. |
| `getTableByName(tableName)` | `IcebergTable \| null` | Retrieves an IcebergTable instance by name. |

### `IcebergTable`

| Method | Return | Description |
| :--- | :--- | :--- |
| `getName()` | `string` | Returns the table name. |
| `getFullPath()` | `string` | Returns the fully qualified table path for SQL statements. |
| `asOf(timestamp)` | `IcebergTable` | Sets snapshot time for Time Travel queries (method chaining). |
| `resetSnapshot()` | `IcebergTable` | Clears Time Travel snapshot constraints. |
| `getValues(filter)` | `Array<Array<any>>` | Queries rows matching conditions and returns a 2D array (headers included). |
| `insertValues(values)` | `number` | Inserts a 2D array of values (first row as column headers). |
| `update(setClause, whereClause)` | `string` | Performs an atomic DML update on matching rows. |
| `deleteRows(whereClause)` | `string` | Deletes rows satisfying the specified predicate. |
| `exportToSheet(sheet, startA1, filter)` | `number` | Directly exports query results into a target Google Spreadsheet sheet. |
| `remove(ifExists)` | `string` | Drops the table entity from the catalog. |

---

## Usage Examples

### 1. Initialize and Create an Iceberg Table

```javascript
const PROJECT_ID = "your-gcp-project-id";
const CATALOG_NAME = "lakehouse_catalog";
const REGION = "asia-northeast1"; // Crucial: Align BigQuery and GCS region

const app = IcebergApp.openByCatalog(PROJECT_ID, CATALOG_NAME, REGION);

const schema = [
  { name: "id", type: "INT64", mode: "REQUIRED" },
  { name: "product", type: "STRING" },
  { name: "price", type: "FLOAT64" },
  { name: "stock", type: "INT64" },
  { name: "created_at", type: "TIMESTAMP" },
];

const table = app.create("SensorsTable", {
  schema: schema,
  storageUri: "gs://your-lakehouse-bucket/tables/SensorsTable",
  partitionBy: ["DATE(created_at)"],
  clusterBy: ["id"],
});
```

### 2. Insert 2D Array Values (Google Sheets Compatible)

```javascript
const records = [
  ["id", "product", "price", "stock", "created_at"],
  [101, "Quantum Sensor Alpha", 1500.0, 10, new Date()],
  [102, "Superconducting Coil", 3200.5, 5, new Date()],
  [103, "Optical Waveguide", 450.0, 42, new Date()],
];

const inserted = table.insertValues(records);
console.log(`Inserted ${inserted} rows.`);
```

### 3. Query with Predicate Pushdown

```javascript
// Iceberg metadata prunes non-matching files prior to scanning
const filtered = table.getValues({
  columns: ["id", "product", "price"],
  where: "price > 1000.0",
});

console.log(filtered);
// Output: [["id", "product", "price"], ["101", "Quantum Sensor Alpha", "1500.0"], ["102", "Superconducting Coil", "3200.5"]]
```

### 4. Direct Export to Google Sheets

```javascript
const ss = SpreadsheetApp.create("Iceberg_Metrics_Export");
const sheet = ss.getSheets()[0];

table.exportToSheet(sheet, "A1", {
  where: "stock > 0",
});

console.log(`Exported to: ${ss.getUrl()}`);
```

### 5. In-Place Mutation and Deletion (ACID DML)

```javascript
// Atomic DML update
const updateRes = table.update("stock = stock - 2", "id = 101");
console.log(updateRes);

// Atomic predicate-based deletion
const delRes = table.deleteRows("stock <= 0");
console.log(delRes);
```

### 6. Snapshot Isolation / Time Travel

```javascript
// Query the state of the table as it existed 10 minutes ago
const pastTime = new Date(Date.now() - 10 * 60 * 1000);
const pastValues = table.asOf(pastTime).getValues({ where: "id = 101" });

console.log(pastValues);
table.resetSnapshot(); // Return to HEAD
```

### 7. Agentic AI Integration via Model Context Protocol (MCP) & [GASADK](https://github.com/tanaikech/adk-gas)

IcebergApp can be effortlessly exposed as deterministic MCP tools hosted directly inside Google Apps Script using **[GASADK (Agent Development Kit for Google Apps Script)](https://github.com/tanaikech/adk-gas)**:

- **Autonomous Tool Calling**: AI foundation models such as **Google Gemini** can dynamically discover and invoke IcebergApp methods (`getValues`, `insertValues`, `update`, `deleteRows`, `asOf`) via standard MCP RPC.
- **Natural Language to Optimized Lakehouse DML**: Business users interact with Gemini in natural language (e.g., *"Analyze sales anomalies across our IoT sensor metrics and adjust flagged calibration offsets"*). Gemini formulates queries, pushes down metadata filters through IcebergApp, and executes ACID DML updates without manual SQL intervention.
- **Closed-Loop Workspace Automation**: Autonomous agents can simultaneously update the Iceberg lakehouse, append audit records into Google Sheets, send Gmail notifications, and generate formatted executive briefs in Google Docs.

---

## Verification & Test Execution

The test suite operates in a **Pure Lifecycle Mode**: it autonomously provisions all required infrastructure resources (BigQuery Dataset and GCS Bucket), runs end-to-end integration tests, and guarantees zero-residue cleanup in the `finally` block.

> [!NOTE]
> Ensure your `appsscript.json` is configured with the [Test Suite Runner Profile](#test-suite-runner-profile-srctestjs) containing all 5 required OAuth scopes (`bigquery`, `spreadsheets`, `drive`, `devstorage.read_write`, and `script.external_request`) before executing `runIcebergAppTests()`.

```
00:00:00  Notice  Execution started
00:00:00  Info    🚀 Starting IcebergApp Automated Test Suite (Stage 3/4 Protocol 17 Compliance)
00:00:00  Info    --- STEP 0-A: Ensuring Isolated Dataset [lakehouse_test_[test-run-id]] ---
00:00:01  Info    ⚡ Dataset [lakehouse_test_[test-run-id]] absent. Provisioning at [asia-northeast1]...
00:00:03  Info    ✅ Dataset [lakehouse_test_[test-run-id]] created at [asia-northeast1].
00:00:03  Info    --- STEP 0-B: Ensuring Ephemeral Bucket [lakehouse-iceberg-test-[project-id]-[test-run-id]] ---
00:00:05  Info    ✅ Base Storage Ready: gs://lakehouse-iceberg-test-[project-id]-[test-run-id]
00:00:05  Info    --- STEP 1: Creating Apache Iceberg Table ---
00:00:06  Info    ✅ Table Created: Test_Iceberg_[test-run-id]
00:00:06  Info    --- STEP 2: Inserting 2D Array Values ---
00:00:07  Info    ✅ Inserted 3 records.
00:00:07  Info    --- STEP 3: Querying with Predicate Pushdown ---
00:00:08  Info    ✅ Predicate pushdown assertions passed: [["id","product","price"],["101","Quantum Sensor Alpha","1500.0"],["102","Superconducting Coil","3200.5"]]
00:00:08  Info    --- STEP 4: Verifying Snapshot Isolation (asOf) ---
00:00:12  Info    --- STEP 5: DML Update ---
00:00:14  Info    ✅ Table Test_Iceberg_[test-run-id] successfully updated.
00:00:15  Info    --- STEP 5-B: Executing Time Travel Query ---
00:00:15  Info    ✅ Time travel snapshot verified successfully.
00:00:15  Info    --- STEP 6: DML Delete ---
00:00:17  Info    ✅ Rows deleted matching (id = 103).
00:00:18  Info    ✅ DML Delete verified.
00:00:18  Info    --- STEP 7: Exporting to Spreadsheet ---
00:00:20  Info    ✅ Exported to Spreadsheet: https://docs.google.com/spreadsheets/d/[spreadsheet-id]/edit
00:00:20  Info    🎉 ALL TESTS & PROTOCOL 17 ASSERTIONS PASSED CLEANLY.
00:00:20  Info    --- ABSOLUTE CLEANUP: Purging ephemeral test resources ---
00:00:20  Info    🗑️ Dropped Iceberg table: Test_Iceberg_[test-run-id]
00:00:21  Info    🗑️ Trashed temporary Spreadsheet: [spreadsheet-id]
00:00:22  Info    🗑️ Removed Ephemeral BigQuery Dataset: [lakehouse_test_[test-run-id]]
00:00:24  Info    🗑️ Deleted Ephemeral GCS Bucket: gs://lakehouse-iceberg-test-[project-id]-[test-run-id]
00:00:24  Info    ✨ CLEANUP COMPLETED: Workspace restored to pure state.
00:00:25  Notice  Execution completed
```

---

## Update History

- **v1.0.0 (September 6, 2026)**
  - Initial release of `IcebergApp`.
  - Support for Lakehouse for Apache Iceberg table management via BigQuery Advanced Service.
  - Native 2D array integration for Google Sheets (`insertValues`, `getValues`, `exportToSheet`).
  - Support for Predicate Pushdown, ACID DML operations (`UPDATE`, `DELETE`), and Snapshot Time Travel (`asOf`).
  - Pure Lifecycle Test Suite with zero-residue autonomous provisioning and cleanup.
  - Full pagination handling, Stage 3 extreme numerical vector resilience, and auto-expanding spreadsheet boundaries.
  - Seamless Model Context Protocol (MCP) tool interoperability via [GASADK](https://github.com/tanaikech/adk-gas) for autonomous AI agent workflows.

---

## Licence

[MIT](LICENSE)

## Author

[Kanshi Tanaike](https://tanaikech.github.io/)

[Donate](https://tanaikech.github.io/donate/)

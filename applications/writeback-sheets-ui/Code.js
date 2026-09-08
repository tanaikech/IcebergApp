/**
 * GitHub: https://github.com/tanaikech/IcebergApp
 * applications/writeback-sheets-ui/Code.js
 *
 * Apache Iceberg End-User Writeback UI via Google Sheets.
 * Interactive Bidirectional Synchronization between Google Workspace and Serverless Lakehouse.
 *
 * Core Specification:
 * - Strictly powered by the IcebergApp core engine (src/IcebergApp.js).
 * - 10-Step Interactive Lifecycle (Menu, Setup, Provisioning, Query, CDC Writeback, Safeguards, Purge)
 * - Dynamic Header-to-Schema Mapping & In-Memory CDC (Added, Modified, Deleted)
 * - Single Atomic MERGE INTO SQL Synthesis with Explicit Type Casting & String Sanitization
 * - Optimistic Concurrency Control (OCC) Timestamp Verification
 * - 100,000-Cell Safety Guard for Spreadsheet Export
 * - Zero-Residue Infrastructure Teardown
 *
 * Copyright (c) 2026 Kanshi Tanaike / tanaike-lab
 * Licensed under the MIT License
 */

/* ========================================================================= */
/*                          GLOBAL CONSTANTS & KEYS                          */
/* ========================================================================= */

const PROP_KEY_PROJECT_ID = "ICEBERG_PROJECT_ID";
const PROP_KEY_REGION = "ICEBERG_REGION";
const PROP_KEY_DATASET_ID = "ICEBERG_DATASET_ID";
const PROP_KEY_BUCKET_NAME = "ICEBERG_BUCKET_NAME";
const PROP_KEY_TABLE_NAME = "ICEBERG_TABLE_NAME";
const PROP_KEY_TABLE_READY = "ICEBERG_TABLE_READY";

const SHEET_DEFAULT_DATA = "default_data";
const SHEET_QUERIED_DATA = "queried_data";
const SHEET_CURRENT_DATA = "current_data";
const SHEET_BASELINE = "__iceberg_baseline__";

const DEFAULT_REGION = "asia-northeast1";
const DEFAULT_TABLE_NAME = "products";
const CELL_LIMIT_THRESHOLD = 100000;

/* ========================================================================= */
/*                      SPREADSHEET MENU & INITIALIZATION                    */
/* ========================================================================= */

/**
 * Installs custom menu upon spreadsheet opening.
 * @param {GoogleAppsScript.Events.SheetsOnOpen} e
 */
function onOpen(e) {
  SpreadsheetApp.getUi()
    .createMenu("Iceberg Lakehouse")
    .addItem("Open Lakehouse Console", "openLakehouseConsole")
    .addSeparator()
    .addItem("Configure GCP Settings", "promptGcpConfig_")
    .addToUi();
}

/**
 * Opens the modern dark-themed sidebar console.
 */
/**
 * Resolves GCP Project ID and Region across ScriptProperties and UserProperties.
 * Supports variations: ICEBERG_PROJECT_ID, PROP_KEY_PROJECT_ID, PROJECT_ID, GCP_PROJECT_ID.
 * @return {{projectId: string, region: string}}
 */
function resolveGcpConfig_() {
  const projectKeys = ["PROJECT_ID", "ICEBERG_PROJECT_ID", "PROP_KEY_PROJECT_ID", "GCP_PROJECT_ID"];
  const regionKeys = ["REGION", "ICEBERG_REGION", "PROP_KEY_REGION", "GCP_REGION"];

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();

  let resolvedProject = "";
  let resolvedRegion = "";

  for (const k of projectKeys) {
    const val = scriptProps.getProperty(k) || userProps.getProperty(k);
    if (val && val.trim() !== "" && val !== "your-gcp-project-id") {
      resolvedProject = val.trim();
      break;
    }
  }

  for (const k of regionKeys) {
    const val = scriptProps.getProperty(k) || userProps.getProperty(k);
    if (val && val.trim() !== "") {
      resolvedRegion = val.trim();
      break;
    }
  }

  return {
    projectId: resolvedProject,
    region: resolvedRegion || DEFAULT_REGION
  };
}

/**
 * Opens the modern dark-themed sidebar console.
 */
function openLakehouseConsole() {
  // Pre-flight check: Verify IcebergApp core engine presence
  verifyIcebergAppEngine_();

  const cfg = resolveGcpConfig_();
  let projectId = cfg.projectId;
  let region = cfg.region;

  if (!projectId) {
    const configured = promptGcpConfig_();
    if (!configured) {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        "GCP Configuration is required to proceed.",
        "Setup Warning",
        6
      );
      return;
    }
  }

  const html = HtmlService.createHtmlOutputFromFile("Sidebar")
    .setTitle("Iceberg Lakehouse Console")
    .setWidth(420);
  SpreadsheetApp.getUi().showSidebar(html);
}

/**
 * Validates that src/IcebergApp.js is available in the GAS environment.
 * @private
 */
function verifyIcebergAppEngine_() {
  const isAvailable = (typeof IcebergApp !== "undefined" && typeof IcebergApp.openByCatalog === "function") ||
                      (typeof openByCatalog === "function");
  if (!isAvailable) {
    const msg = "Mandatory Dependency Missing: IcebergApp core engine (src/IcebergApp.js) is required. " +
                "Please add 'src/IcebergApp.js' to this Google Apps Script project or link the IcebergApp library.";
    SpreadsheetApp.getUi().alert("Missing IcebergApp Engine", msg, SpreadsheetApp.getUi().ButtonSet.OK);
    throw new Error(msg);
  }
}

/**
 * Prompts user for GCP Project ID and Region.
 * @return {boolean} True if successfully configured.
 * @private
 */
function promptGcpConfig_() {
  const ui = SpreadsheetApp.getUi();
  const currentCfg = resolveGcpConfig_();
  const existingProject = currentCfg.projectId || "";
  const existingRegion = currentCfg.region || DEFAULT_REGION;

  const projectRes = ui.prompt(
    "GCP Project Configuration",
    `Enter your Google Cloud Project ID:\n(Current: ${existingProject || "None"})`,
    ui.ButtonSet.OK_CANCEL
  );

  if (projectRes.getSelectedButton() !== ui.Button.OK) {
    return false;
  }
  const enteredProject = projectRes.getResponseText().trim();
  if (!enteredProject) {
    ui.alert("Invalid Input", "GCP Project ID cannot be empty.", ui.ButtonSet.OK);
    return false;
  }

  const regionRes = ui.prompt(
    "GCP Region Configuration",
    `Enter Lakehouse Region (BigQuery & GCS):\n(Default: ${existingRegion})`,
    ui.ButtonSet.OK_CANCEL
  );

  let enteredRegion = DEFAULT_REGION;
  if (regionRes.getSelectedButton() === ui.Button.OK) {
    const customRegion = regionRes.getResponseText().trim();
    if (customRegion) {
      enteredRegion = customRegion;
    }
  }

  // Save to both ScriptProperties and UserProperties for seamless multi-mode execution
  PropertiesService.getScriptProperties().setProperty("PROJECT_ID", enteredProject);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_PROJECT_ID, enteredProject);
  PropertiesService.getScriptProperties().setProperty("REGION", enteredRegion);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_REGION, enteredRegion);
  PropertiesService.getUserProperties().setProperty("PROJECT_ID", enteredProject);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_PROJECT_ID, enteredProject);
  PropertiesService.getUserProperties().setProperty("REGION", enteredRegion);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_REGION, enteredRegion);

  SpreadsheetApp.getActiveSpreadsheet().toast(
    `Configured Project [${enteredProject}] in Region [${enteredRegion}].`,
    "Configuration Saved",
    5
  );
  return true;
}

/**
 * Returns active state and metadata to the sidebar UI.
 * @return {Object} Status configuration object.
 */
function getSpreadsheetConfig() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();
  const datasetId = scriptProps.getProperty(PROP_KEY_DATASET_ID) || userProps.getProperty(PROP_KEY_DATASET_ID) || "";
  const bucketName = scriptProps.getProperty(PROP_KEY_BUCKET_NAME) || userProps.getProperty(PROP_KEY_BUCKET_NAME) || "";
  const tableName = scriptProps.getProperty(PROP_KEY_TABLE_NAME) || userProps.getProperty(PROP_KEY_TABLE_NAME) || DEFAULT_TABLE_NAME;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const hasDefaultData = ss.getSheetByName(SHEET_DEFAULT_DATA) !== null;
  const hasQueriedData = ss.getSheetByName(SHEET_QUERIED_DATA) !== null;
  const hasCurrentData = ss.getSheetByName(SHEET_CURRENT_DATA) !== null;

  const isTableReady = (scriptProps.getProperty(PROP_KEY_TABLE_READY) || userProps.getProperty(PROP_KEY_TABLE_READY)) === "true";

  return {
    projectId: projectId,
    region: region,
    datasetId: datasetId,
    bucketName: bucketName,
    tableName: tableName,
    isConfigured: !!projectId,
    isInfraReady: !!(datasetId && bucketName),
    isTableReady: isTableReady,
    hasDefaultData: hasDefaultData,
    hasQueriedData: hasQueriedData,
    hasCurrentData: hasCurrentData,
    defaultQuery: datasetId
      ? `SELECT * FROM \`${projectId}.${datasetId}.${tableName}\` WHERE price > 1000.0 ORDER BY id ASC`
      : `SELECT * FROM \`products\` WHERE price > 1000.0 ORDER BY id ASC`
  };
}

/**
 * Saves or updates GCP configuration from the sidebar.
 * @param {string} projectId
 * @param {string} region
 * @return {Object} Updated status.
 */
function saveSpreadsheetConfig(projectId, region) {
  if (!projectId || typeof projectId !== "string" || !projectId.trim()) {
    throw new Error("Project ID is required.");
  }
  const cleanProject = projectId.trim();
  const cleanRegion = (region && region.trim()) || DEFAULT_REGION;

  PropertiesService.getScriptProperties().setProperty("PROJECT_ID", cleanProject);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_PROJECT_ID, cleanProject);
  PropertiesService.getScriptProperties().setProperty("REGION", cleanRegion);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_REGION, cleanRegion);
  PropertiesService.getUserProperties().setProperty("PROJECT_ID", cleanProject);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_PROJECT_ID, cleanProject);
  PropertiesService.getUserProperties().setProperty("REGION", cleanRegion);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_REGION, cleanRegion);

  return getSpreadsheetConfig();
}

/* ========================================================================= */
/*                   STEP 4: INFRASTRUCTURE & DATA STAGING                   */
/* ========================================================================= */

/**
 * Step 4: Provisions BigQuery dataset, Cloud Storage bucket, and seeds default_data sheet.
 * @return {Object} Execution result.
 */
function stepInitInfra() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  if (!projectId) {
    throw new Error("GCP Project ID is not configured. Please configure project settings.");
  }

  const timestamp = new Date().getTime();
  const datasetId = `lakehouse_writeback_${timestamp}`;
  const cleanProj = projectId.toLowerCase().replace(/[^a-z0-9_-]/g, "");
  const bucketName = `lakehouse-iceberg-wb-${cleanProj}-${timestamp}`;

  // 1. Provision BigQuery Dataset
  ensureBigQueryDataset_(projectId, datasetId, region);

  // 2. Provision GCS Bucket
  const storageUri = ensureGcsBucket_(projectId, bucketName, region);

  // Persist infrastructure identifiers in both ScriptProperties and UserProperties
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_DATASET_ID, datasetId);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_BUCKET_NAME, bucketName);
  PropertiesService.getScriptProperties().setProperty(PROP_KEY_TABLE_NAME, DEFAULT_TABLE_NAME);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_DATASET_ID, datasetId);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_BUCKET_NAME, bucketName);
  PropertiesService.getUserProperties().setProperty(PROP_KEY_TABLE_NAME, DEFAULT_TABLE_NAME);

  // 3. Stage 5x20 realistic sample records in default_data sheet
  stageDefaultData_();

  return {
    success: true,
    message: `Infrastructure initialized: Dataset [${datasetId}], Bucket [${bucketName}]. Sample data staged in '${SHEET_DEFAULT_DATA}'.`,
    config: getSpreadsheetConfig()
  };
}

/**
 * Stages a realistic 5-column by 20-row sample dataset into default_data sheet.
 * @private
 */
function stageDefaultData_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_DEFAULT_DATA);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_DEFAULT_DATA);
  } else {
    sheet.clear();
    sheet.clearFormats();
  }

  const now = new Date();
  const baseIso = now.toISOString();

  const headers = ["id", "product", "price", "stock", "updated_at"];
  const rows = [
    [101, "Quantum Sensor Alpha", 1500.0, 10, baseIso],
    [102, "Optical Waveguide MK-2", 450.0, 42, baseIso],
    [103, "Cryogenic Valve X1", 3200.5, 5, baseIso],
    [104, "Superconducting Resonator", 4800.0, 3, baseIso],
    [105, "Photonic Switch Array", 2100.0, 15, baseIso],
    [106, "Silicon Photonic Modulator", 890.0, 60, baseIso],
    [107, "Laser Diode Collimator", 340.0, 120, baseIso],
    [108, "Fiber Bragg Grating Sensor", 620.0, 35, baseIso],
    [109, "Terahertz Detector Core", 5400.0, 4, baseIso],
    [110, "Graphene Field-Effect Probe", 1250.0, 25, baseIso],
    [111, "Superfluid Helium Exchanger", 6800.0, 2, baseIso],
    [112, "Vacuum Ionization Gauge", 780.0, 18, baseIso],
    [113, "Precision Piezo Positioner", 1950.0, 12, baseIso],
    [114, "Atomic Clock Frequency Cell", 8200.0, 3, baseIso],
    [115, "RF Shielded Cryostat Pod", 9500.0, 1, baseIso],
    [116, "Avalanche Photodiode Module", 1100.0, 30, baseIso],
    [117, "MEMS Gyroscope Accelerometer", 290.0, 150, baseIso],
    [118, "Magneto-Optical Kerr Mirror", 2400.0, 8, baseIso],
    [119, "Parametric Pulse Amplifier", 4100.0, 6, baseIso],
    [120, "Single-Photon Avalanche Diode", 3750.0, 7, baseIso]
  ];

  const fullData = [headers, ...rows];
  const range = sheet.getRange(1, 1, fullData.length, headers.length);
  range.setValues(fullData);

  // Apply clean styling
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setBackground("#1e293b");
  headerRange.setFontColor("#38bdf8");
  headerRange.setFontWeight("bold");
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, headers.length);

  ss.setActiveSheet(sheet);
}

/* ========================================================================= */
/*               STEP 5: CREATE ICEBERG TABLE (VIA ICEBERGAPP)               */
/* ========================================================================= */

/**
 * Step 5: Creates Apache Iceberg table using IcebergApp library and inserts staged records.
 * @return {Object} Execution result.
 */
function stepCreateTable() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();
  const datasetId = scriptProps.getProperty(PROP_KEY_DATASET_ID) || userProps.getProperty(PROP_KEY_DATASET_ID);
  const bucketName = scriptProps.getProperty(PROP_KEY_BUCKET_NAME) || userProps.getProperty(PROP_KEY_BUCKET_NAME);
  const tableName = scriptProps.getProperty(PROP_KEY_TABLE_NAME) || userProps.getProperty(PROP_KEY_TABLE_NAME) || DEFAULT_TABLE_NAME;

  if (!projectId || !datasetId || !bucketName) {
    throw new Error("Infrastructure not initialized. Please click 'Initialize Infrastructure' first.");
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const defaultSheet = ss.getSheetByName(SHEET_DEFAULT_DATA);
  if (!defaultSheet) {
    throw new Error(`Sheet '${SHEET_DEFAULT_DATA}' not found. Please re-run initialization.`);
  }

  const defaultData = defaultSheet.getDataRange().getValues();
  if (defaultData.length < 2) {
    throw new Error(`Insufficient data in '${SHEET_DEFAULT_DATA}'. Header and data rows required.`);
  }

  const storageUri = `gs://${bucketName}/${tableName}`;
  const schema = [
    { name: "id", type: "INT64", mode: "REQUIRED" },
    { name: "product", type: "STRING" },
    { name: "price", type: "FLOAT64" },
    { name: "stock", type: "INT64" },
    { name: "updated_at", type: "TIMESTAMP" }
  ];

  // Strictly invoke IcebergApp core instance
  const app = getIcebergApp_(projectId, datasetId, region);
  if (typeof app.ensureCatalog === "function") {
    app.ensureCatalog();
  }
  const table = app.create(tableName, {
    schema: schema,
    storageUri: storageUri,
    partitionBy: ["DATE(updated_at)"],
    clusterBy: ["id"]
  });

  // Convert Date strings to Date objects if needed for insertValues
  const sanitizedRows = defaultData.map((row, rIdx) => {
    if (rIdx === 0) return row;
    return row.map((val, cIdx) => {
      if (cIdx === 4 && typeof val === "string") {
        return new Date(val);
      }
      return val;
    });
  });

  const insertedCount = table.insertValues(sanitizedRows);
  const defaultQuery = `SELECT * FROM \`${projectId}.${datasetId}.${tableName}\` WHERE price > 1000.0 ORDER BY id ASC`;

  PropertiesService.getScriptProperties().setProperty(PROP_KEY_TABLE_READY, "true");
  PropertiesService.getUserProperties().setProperty(PROP_KEY_TABLE_READY, "true");

  return {
    success: true,
    message: `Iceberg table '${tableName}' created successfully via IcebergApp with ${insertedCount} rows inserted.`,
    tableName: tableName,
    insertedCount: insertedCount,
    defaultQuery: defaultQuery,
    config: getSpreadsheetConfig()
  };
}

/* ========================================================================= */
/*              STEP 6: QUERY EXECUTION & DATA VALIDATION                    */
/* ========================================================================= */

/**
 * Step 6: Executes query, outputs to queried_data, adds data validation rules, and clones baseline.
 * @param {string} sqlQuery SQL Query to execute.
 * @return {Object} Execution result.
 */
function stepExecuteQuery(sqlQuery) {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  if (!projectId) {
    throw new Error("Project ID is missing. Please configure settings.");
  }
  if (!sqlQuery || !sqlQuery.trim()) {
    throw new Error("Query string cannot be empty.");
  }

  const cleanQuery = sqlQuery.trim();
  const rawValues = runBqJobRawValues_(projectId, cleanQuery, region);

  if (!rawValues || rawValues.length === 0) {
    throw new Error("Query executed but returned zero rows or no schema.");
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // 1. Prepare queried_data sheet
  let querySheet = ss.getSheetByName(SHEET_QUERIED_DATA);
  if (!querySheet) {
    querySheet = ss.insertSheet(SHEET_QUERIED_DATA);
  } else {
    querySheet.clear();
    querySheet.clearFormats();
    querySheet.clearConditionalFormatRules();
  }

  // Ensure sheet dimension
  const numRows = rawValues.length;
  const numCols = rawValues[0].length;
  if (querySheet.getMaxRows() < numRows) {
    querySheet.insertRowsAfter(querySheet.getMaxRows(), numRows - querySheet.getMaxRows());
  }
  if (querySheet.getMaxColumns() < numCols) {
    querySheet.insertColumnsAfter(querySheet.getMaxColumns(), numCols - querySheet.getMaxColumns());
  }

  // Populate data
  const dataRange = querySheet.getRange(1, 1, numRows, numCols);
  dataRange.setValues(rawValues);

  // Style header
  const headerRange = querySheet.getRange(1, 1, 1, numCols);
  headerRange.setBackground("#0f172a");
  headerRange.setFontColor("#38bdf8");
  headerRange.setFontWeight("bold");
  querySheet.setFrozenRows(1);

  // 2. Attach Data Validations to columns
  const headers = rawValues[0].map(h => String(h).trim().toLowerCase());
  const dataRowCount = numRows - 1;

  if (dataRowCount > 0) {
    const priceColIdx = headers.indexOf("price");
    if (priceColIdx !== -1) {
      const priceRange = querySheet.getRange(2, priceColIdx + 1, dataRowCount, 1);
      const rule = SpreadsheetApp.newDataValidation()
        .requireNumberGreaterThan(0)
        .setAllowInvalid(false)
        .setHelpText("Price must be a positive number greater than 0.")
        .build();
      priceRange.setDataValidation(rule);
    }

    const stockColIdx = headers.indexOf("stock");
    if (stockColIdx !== -1) {
      const stockRange = querySheet.getRange(2, stockColIdx + 1, dataRowCount, 1);
      const rule = SpreadsheetApp.newDataValidation()
        .requireNumberGreaterThanOrEqualTo(0)
        .setAllowInvalid(false)
        .setHelpText("Stock must be a non-negative integer (>= 0).")
        .build();
      stockRange.setDataValidation(rule);
    }

    const idColIdx = headers.indexOf("id");
    if (idColIdx !== -1) {
      const idRange = querySheet.getRange(2, idColIdx + 1, dataRowCount, 1);
      const rule = SpreadsheetApp.newDataValidation()
        .requireNumberGreaterThan(0)
        .setAllowInvalid(false)
        .setHelpText("ID must be a positive integer.")
        .build();
      idRange.setDataValidation(rule);
    }
  }

  querySheet.autoResizeColumns(1, numCols);

  // 3. Mirror exact snapshot to hidden __iceberg_baseline__ sheet
  let baselineSheet = ss.getSheetByName(SHEET_BASELINE);
  if (!baselineSheet) {
    baselineSheet = ss.insertSheet(SHEET_BASELINE);
  } else {
    baselineSheet.clear();
    baselineSheet.clearFormats();
  }

  if (baselineSheet.getMaxRows() < numRows) {
    baselineSheet.insertRowsAfter(baselineSheet.getMaxRows(), numRows - baselineSheet.getMaxRows());
  }
  if (baselineSheet.getMaxColumns() < numCols) {
    baselineSheet.insertColumnsAfter(baselineSheet.getMaxColumns(), numCols - baselineSheet.getMaxColumns());
  }
  baselineSheet.getRange(1, 1, numRows, numCols).setValues(rawValues);

  // Protect baseline sheet
  try {
    const protection = baselineSheet.protect();
    protection.setDescription("Immutable Iceberg CDC Baseline Snapshot");
    protection.setWarningOnly(true);
  } catch (e) {
    console.warn("Sheet protection skipped: " + e.message);
  }

  // Hide baseline sheet from casual view
  baselineSheet.hideSheet();

  // Activate queried_data sheet
  ss.setActiveSheet(querySheet);

  return {
    success: true,
    rowCount: numRows - 1,
    message: `Query returned ${numRows - 1} data rows. Seeded '${SHEET_QUERIED_DATA}' with Data Validation rules and created hidden baseline snapshot.`,
    config: getSpreadsheetConfig()
  };
}

/* ========================================================================= */
/*              STEP 8: CDC ENGINE & ATOMIC MERGE WRITEBACK                  */
/* ========================================================================= */

/**
 * Step 8: Performs CDC between queried_data and __iceberg_baseline__, then dispatches an atomic MERGE INTO.
 * @return {Object} Execution result with mutation metrics.
 */
function stepCommitChanges() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();
  const datasetId = scriptProps.getProperty(PROP_KEY_DATASET_ID) || userProps.getProperty(PROP_KEY_DATASET_ID);
  const tableName = scriptProps.getProperty(PROP_KEY_TABLE_NAME) || userProps.getProperty(PROP_KEY_TABLE_NAME) || DEFAULT_TABLE_NAME;

  if (!projectId || !datasetId) {
    throw new Error("Infrastructure not initialized. Please ensure Project ID and Dataset ID are configured.");
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const activeSheet = ss.getSheetByName(SHEET_QUERIED_DATA);
  const baselineSheet = ss.getSheetByName(SHEET_BASELINE);

  if (!activeSheet) {
    throw new Error(`Active query sheet '${SHEET_QUERIED_DATA}' not found.`);
  }
  if (!baselineSheet) {
    throw new Error(`Baseline sheet '${SHEET_BASELINE}' not found. Please execute a query first.`);
  }

  const activeValues = activeSheet.getDataRange().getValues();
  const baselineValues = baselineSheet.getDataRange().getValues();

  if (activeValues.length === 0 || baselineValues.length === 0) {
    throw new Error("Queried data or baseline is completely empty.");
  }

  // 1. Dynamic Header Mapping
  const activeHeaders = activeValues[0].map(h => String(h).trim().toLowerCase());
  const baselineHeaders = baselineValues[0].map(h => String(h).trim().toLowerCase());

  const activeIdIdx = activeHeaders.indexOf("id");
  const baselineIdIdx = baselineHeaders.indexOf("id");

  if (activeIdIdx === -1 || baselineIdIdx === -1) {
    throw new Error("Primary Key column 'id' is required in both queried_data and baseline.");
  }

  // Build baseline lookup map by PK
  const baselineMap = new Map();
  for (let r = 1; r < baselineValues.length; r++) {
    const row = baselineValues[r];
    const pk = row[baselineIdIdx];
    if (pk !== null && pk !== "" && pk !== undefined) {
      baselineMap.set(String(pk), {
        row: row,
        rowIndex: r
      });
    }
  }

  const addedRows = [];
  const modifiedRows = [];
  const activePkSet = new Set();
  const nowIso = new Date().toISOString();

  // Scan active sheet for Added and Modified rows
  for (let r = 1; r < activeValues.length; r++) {
    const row = activeValues[r];
    const pk = row[activeIdIdx];
    if (pk === null || pk === "" || pk === undefined) {
      continue; // Skip empty trailing rows
    }
    const pkStr = String(pk);
    activePkSet.add(pkStr);

    if (!baselineMap.has(pkStr)) {
      // ADDED Row
      const rowObj = rowToRecord_(row, activeHeaders);
      const parsedId = Number(rowObj.id);
      if (isNaN(parsedId) || parsedId <= 0) {
        throw new Error(`Invalid ID '${rowObj.id}' in new row. ID must be a positive integer.`);
      }
      rowObj.id = parsedId;
      rowObj.updated_at = rowObj.updated_at || nowIso;
      addedRows.push(rowObj);
    } else {
      // Existing Row: Check for modifications
      const baseEntry = baselineMap.get(pkStr);
      const baseObj = rowToRecord_(baseEntry.row, baselineHeaders);
      const activeObj = rowToRecord_(row, activeHeaders);

      if (isRecordModified_(activeObj, baseObj)) {
        activeObj._orig_updated_at = baseObj.updated_at;
        activeObj.updated_at = nowIso; // Stamp latest commit timestamp
        modifiedRows.push(activeObj);
      }
    }
  }

  // Scan baseline for Deleted rows
  const deletedRows = [];
  for (let [pkStr, baseEntry] of baselineMap.entries()) {
    if (!activePkSet.has(pkStr)) {
      const baseObj = rowToRecord_(baseEntry.row, baselineHeaders);
      baseObj._orig_updated_at = baseObj.updated_at;
      deletedRows.push(baseObj);
    }
  }

  const totalMutations = addedRows.length + modifiedRows.length + deletedRows.length;
  if (totalMutations === 0) {
    return {
      success: true,
      added: 0,
      modified: 0,
      deleted: 0,
      affectedRows: 0,
      message: "No changes detected. Lakehouse is already completely synchronized."
    };
  }

  // 2. Synthesize Atomic Single MERGE INTO SQL Statement
  const fullTablePath = `\`${projectId}.${datasetId}.${tableName}\``;
  const mergeSql = buildMergeIntoSql_(fullTablePath, addedRows, modifiedRows, deletedRows);

  // 3. Execute MERGE INTO Statement via BigQuery
  const bqRes = runBqJob_(projectId, mergeSql, region);
  const affectedCount = bqRes.affectedRows;

  // 4. Concurrency & Integrity Assertions (Optimistic Concurrency Control)
  if (affectedCount < totalMutations) {
    throw new Error(
      `Concurrency Conflict: Expected ${totalMutations} mutations (${addedRows.length} added, ${modifiedRows.length} modified, ${deletedRows.length} deleted), ` +
      `but BigQuery affected ${affectedCount} rows. Another process or user modified the table concurrently, or timestamps diverged. ` +
      `Please re-run query in Step 3 to obtain the latest lakehouse snapshot.`
    );
  }

  // 5. Synchronize Baseline Snapshot to Match Active State
  baselineSheet.clear();
  const newActiveValues = activeSheet.getDataRange().getValues();
  if (baselineSheet.getMaxRows() < newActiveValues.length) {
    baselineSheet.insertRowsAfter(baselineSheet.getMaxRows(), newActiveValues.length - baselineSheet.getMaxRows());
  }
  if (baselineSheet.getMaxColumns() < newActiveValues[0].length) {
    baselineSheet.insertColumnsAfter(baselineSheet.getMaxColumns(), newActiveValues[0].length - baselineSheet.getMaxColumns());
  }
  baselineSheet.getRange(1, 1, newActiveValues.length, newActiveValues[0].length).setValues(newActiveValues);

  return {
    success: true,
    added: addedRows.length,
    modified: modifiedRows.length,
    deleted: deletedRows.length,
    affectedRows: affectedCount,
    message: `Committed successfully: ${addedRows.length} added, ${modifiedRows.length} updated, ${deletedRows.length} deleted (Total DML affected rows: ${affectedCount}).`
  };
}

/**
 * Converts a sheet row array to a standardized record object.
 * @private
 */
function rowToRecord_(row, headers) {
  const obj = {};
  headers.forEach((h, idx) => {
    let val = row[idx];
    if (val instanceof Date) {
      val = val.toISOString();
    } else if (h === "updated_at" && val !== null && val !== "" && val !== undefined) {
      if (typeof val === "number") {
        const millis = val < 10000000000 ? val * 1000 : val;
        const d = new Date(millis);
        if (!isNaN(d.getTime())) {
          val = d.toISOString();
        }
      } else if (typeof val === "string") {
        const numVal = parseFloat(val);
        if (!isNaN(numVal) && String(numVal) === val.trim() && numVal > 1000000) {
          const millis = numVal < 10000000000 ? numVal * 1000 : numVal;
          const d = new Date(millis);
          if (!isNaN(d.getTime())) {
            val = d.toISOString();
          }
        }
      }
    }
    obj[h] = val;
  });
  return obj;
}

/**
 * Compares two records across common fields with numeric tolerance.
 * @private
 */
function isRecordModified_(activeObj, baseObj) {
  const checkFields = ["product", "price", "stock"];
  for (let f of checkFields) {
    if (activeObj[f] !== undefined && baseObj[f] !== undefined) {
      if (f === "price") {
        const n1 = parseFloat(activeObj[f]);
        const n2 = parseFloat(baseObj[f]);
        if (!isNaN(n1) && !isNaN(n2)) {
          if (Math.abs(n1 - n2) > 0.000001) return true;
          continue;
        }
      } else if (f === "stock") {
        const s1 = parseInt(activeObj[f], 10);
        const s2 = parseInt(baseObj[f], 10);
        if (!isNaN(s1) && !isNaN(s2)) {
          if (s1 !== s2) return true;
          continue;
        }
      }
      const v1 = String(activeObj[f]).trim();
      const v2 = String(baseObj[f]).trim();
      if (v1 !== v2) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Synthesizes atomic MERGE INTO SQL query with explicit casting and escaping.
 * @private
 */
function buildMergeIntoSql_(targetTablePath, addedRows, modifiedRows, deletedRows) {
  const unionBranches = [];

  // Branch 1: Added rows (INSERT)
  addedRows.forEach((r) => {
    const idVal = Number(r.id);
    const prodVal = escapeSqlString_(r.product);
    const priceVal = Number.isFinite(Number(r.price)) ? Number(r.price) : "CAST(NULL AS FLOAT64)";
    const stockVal = Number.isFinite(Number(r.stock)) ? Math.floor(Number(r.stock)) : "CAST(NULL AS INT64)";
    const updatedVal = formatTimestampSql_(r.updated_at);

    unionBranches.push(
      `SELECT CAST(${idVal} AS INT64) AS id, CAST('${prodVal}' AS STRING) AS product, CAST(${priceVal} AS FLOAT64) AS price, CAST(${stockVal} AS INT64) AS stock, ${updatedVal} AS updated_at, 'INSERT' AS _action, TIMESTAMP '1970-01-01T00:00:00Z' AS _orig_updated_at`
    );
  });

  // Branch 2: Modified rows (UPDATE)
  modifiedRows.forEach((r) => {
    const idVal = Number(r.id);
    const prodVal = escapeSqlString_(r.product);
    const priceVal = Number.isFinite(Number(r.price)) ? Number(r.price) : "CAST(NULL AS FLOAT64)";
    const stockVal = Number.isFinite(Number(r.stock)) ? Math.floor(Number(r.stock)) : "CAST(NULL AS INT64)";
    const updatedVal = formatTimestampSql_(r.updated_at);
    const origUpdatedVal = formatTimestampSql_(r._orig_updated_at);

    unionBranches.push(
      `SELECT CAST(${idVal} AS INT64) AS id, CAST('${prodVal}' AS STRING) AS product, CAST(${priceVal} AS FLOAT64) AS price, CAST(${stockVal} AS INT64) AS stock, ${updatedVal} AS updated_at, 'UPDATE' AS _action, ${origUpdatedVal} AS _orig_updated_at`
    );
  });

  // Branch 3: Deleted rows (DELETE)
  deletedRows.forEach((r) => {
    const idVal = Number(r.id);
    const origUpdatedVal = formatTimestampSql_(r._orig_updated_at);

    unionBranches.push(
      `SELECT CAST(${idVal} AS INT64) AS id, CAST(NULL AS STRING) AS product, CAST(NULL AS FLOAT64) AS price, CAST(NULL AS INT64) AS stock, TIMESTAMP '1970-01-01T00:00:00Z' AS updated_at, 'DELETE' AS _action, ${origUpdatedVal} AS _orig_updated_at`
    );
  });

  const sourceSql = unionBranches.join("\nUNION ALL\n");

  return `
MERGE INTO ${targetTablePath} AS T
USING (
${sourceSql}
) AS S
ON T.id = S.id
WHEN MATCHED AND S._action = 'UPDATE' AND (TIMESTAMP_DIFF(T.updated_at, S._orig_updated_at, SECOND) = 0 OR S._orig_updated_at IS NULL OR S._orig_updated_at = TIMESTAMP '1970-01-01T00:00:00Z' OR T.updated_at IS NULL) THEN
  UPDATE SET T.product = S.product, T.price = S.price, T.stock = S.stock, T.updated_at = S.updated_at
WHEN MATCHED AND S._action = 'DELETE' AND (TIMESTAMP_DIFF(T.updated_at, S._orig_updated_at, SECOND) = 0 OR S._orig_updated_at IS NULL OR S._orig_updated_at = TIMESTAMP '1970-01-01T00:00:00Z' OR T.updated_at IS NULL) THEN
  DELETE
WHEN NOT MATCHED AND S._action = 'INSERT' THEN
  INSERT (id, product, price, stock, updated_at)
  VALUES (S.id, S.product, S.price, S.stock, S.updated_at);
`;
}

/**
 * Escapes text for SQL string literals.
 * @private
 */
function escapeSqlString_(val) {
  if (val === null || val === undefined) return "";
  return String(val).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Formats a timestamp into a BigQuery TIMESTAMP literal.
 * Supports ISO strings, Date instances, and epoch seconds/millis numbers.
 * @private
 */
function formatTimestampSql_(val) {
  if (!val) return "TIMESTAMP '1970-01-01T00:00:00Z'";
  if (val instanceof Date) {
    return isNaN(val.getTime()) ? "TIMESTAMP '1970-01-01T00:00:00Z'" : `TIMESTAMP('${val.toISOString()}')`;
  }
  if (typeof val === "number") {
    const millis = val < 10000000000 ? val * 1000 : val;
    const d = new Date(millis);
    return isNaN(d.getTime()) ? "TIMESTAMP '1970-01-01T00:00:00Z'" : `TIMESTAMP('${d.toISOString()}')`;
  }
  if (typeof val === "string") {
    const numVal = parseFloat(val);
    if (!isNaN(numVal) && String(numVal) === val.trim() && numVal > 1000000) {
      const millis = numVal < 10000000000 ? numVal * 1000 : numVal;
      const d = new Date(millis);
      return isNaN(d.getTime()) ? "TIMESTAMP '1970-01-01T00:00:00Z'" : `TIMESTAMP('${d.toISOString()}')`;
    }
    const d = new Date(val);
    if (!isNaN(d.getTime())) {
      return `TIMESTAMP('${d.toISOString()}')`;
    }
  }
  return "TIMESTAMP '1970-01-01T00:00:00Z'";
}

/* ========================================================================= */
/*              STEP 9: GET ALL CURRENT DATA & 100K SAFETY GUARD             */
/* ========================================================================= */

/**
 * Step 9: Fetches complete table content with 100,000-cell guard into current_data sheet via IcebergApp.
 * @return {Object} Result object.
 */
function stepGetAllCurrentData() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();
  const datasetId = scriptProps.getProperty(PROP_KEY_DATASET_ID) || userProps.getProperty(PROP_KEY_DATASET_ID);
  const tableName = scriptProps.getProperty(PROP_KEY_TABLE_NAME) || userProps.getProperty(PROP_KEY_TABLE_NAME) || DEFAULT_TABLE_NAME;

  if (!projectId || !datasetId) {
    throw new Error("Infrastructure not initialized. Please ensure Project ID and Dataset ID are configured.");
  }

  // Pre-flight check: count total rows and calculate cell count
  const countSql = `SELECT COUNT(*) AS total_rows FROM \`${projectId}.${datasetId}.${tableName}\``;
  const countRes = runBqJob_(projectId, countSql, region);
  const totalRows = Number(countRes.rows[0].total_rows || 0);

  const columnCount = 5; // id, product, price, stock, updated_at
  const totalCells = totalRows * columnCount;

  if (totalCells > CELL_LIMIT_THRESHOLD) {
    throw new Error(
      `Operation Blocked: Dataset exceeds the 100,000-cell safety limit (Found ${totalCells} cells: ${totalRows} rows x ${columnCount} columns). ` +
      `Please use filtered queries to inspect data.`
    );
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let currentSheet = ss.getSheetByName(SHEET_CURRENT_DATA);
  if (!currentSheet) {
    currentSheet = ss.insertSheet(SHEET_CURRENT_DATA);
  } else {
    currentSheet.clear();
    currentSheet.clearFormats();
  }

  // Strictly utilize IcebergApp core instance
  const app = getIcebergApp_(projectId, datasetId, region);
  const table = app.getTableByName(tableName);
  if (!table) {
    throw new Error(`Iceberg table '${tableName}' was not found in catalog.`);
  }

  const writtenRows = table.exportToSheet(currentSheet, "A1", { orderBy: "id ASC" });

  // Format header
  const headerRange = currentSheet.getRange(1, 1, 1, columnCount);
  headerRange.setBackground("#1e293b");
  headerRange.setFontColor("#38bdf8");
  headerRange.setFontWeight("bold");
  currentSheet.setFrozenRows(1);
  currentSheet.autoResizeColumns(1, columnCount);

  // Ensure strict ascending sort by ID on sheet level
  if (writtenRows > 2) {
    const headers = headerRange.getValues()[0];
    const idColIdx = headers.indexOf("id");
    if (idColIdx !== -1) {
      currentSheet.getRange(2, 1, writtenRows - 1, columnCount).sort({ column: idColIdx + 1, ascending: true });
    }
  }

  ss.setActiveSheet(currentSheet);

  return {
    success: true,
    totalRows: totalRows,
    writtenRows: writtenRows,
    message: `Exported ${writtenRows} rows (sorted by ID ASC) to '${SHEET_CURRENT_DATA}' via IcebergApp. Total cells: ${totalCells}.`
  };
}

/* ========================================================================= */
/*              STEP 10: ALL RESET & ZERO-RESIDUE INFRASTRUCTURE PURGE      */
/* ========================================================================= */

/**
 * Step 10: Completely removes table, bucket, dataset, and local test sheets.
 * @return {Object} Result object.
 */
function stepResetAndPurge() {
  const cfg = resolveGcpConfig_();
  const projectId = cfg.projectId;
  const region = cfg.region;

  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();
  const datasetId = scriptProps.getProperty(PROP_KEY_DATASET_ID) || userProps.getProperty(PROP_KEY_DATASET_ID);
  const bucketName = scriptProps.getProperty(PROP_KEY_BUCKET_NAME) || userProps.getProperty(PROP_KEY_BUCKET_NAME);
  const tableName = scriptProps.getProperty(PROP_KEY_TABLE_NAME) || userProps.getProperty(PROP_KEY_TABLE_NAME) || DEFAULT_TABLE_NAME;

  const logs = [];

  // 1. Drop Iceberg Table via IcebergApp
  if (projectId && datasetId && tableName) {
    try {
      const app = getIcebergApp_(projectId, datasetId, region);
      const table = app.getTableByName(tableName);
      if (table) {
        table.remove(true);
        logs.push(`Dropped Iceberg table '${tableName}' via IcebergApp.`);
      }
    } catch (e) {
      logs.push(`Table drop skipped: ${e.message}`);
    }
  }

  // 2. Delete GCS Bucket Completely
  if (bucketName) {
    try {
      deleteGcsBucketCompletely_(bucketName);
      logs.push(`Deleted GCS bucket 'gs://${bucketName}'.`);
    } catch (e) {
      logs.push(`GCS bucket delete skipped: ${e.message}`);
    }
  }

  // 3. Remove BigQuery Dataset
  if (projectId && datasetId) {
    try {
      BigQuery.Datasets.remove(projectId, datasetId, { deleteContents: true });
      logs.push(`Removed BigQuery dataset '${datasetId}'.`);
    } catch (e) {
      logs.push(`Dataset removal skipped: ${e.message}`);
    }
  }

  // 4. Clean up Google Sheets tabs
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetsToPurge = [SHEET_DEFAULT_DATA, SHEET_QUERIED_DATA, SHEET_CURRENT_DATA, SHEET_BASELINE];
  
  // Ensure at least one sheet remains before deleting
  let fallbackSheet = ss.getSheetByName("Sheet1") || ss.getSheets()[0];
  if (!fallbackSheet || sheetsToPurge.includes(fallbackSheet.getName())) {
    fallbackSheet = ss.insertSheet("Ready");
  }

  sheetsToPurge.forEach((sheetName) => {
    const s = ss.getSheetByName(sheetName);
    if (s && s.getSheetId() !== fallbackSheet.getSheetId()) {
      try {
        ss.deleteSheet(s);
        logs.push(`Removed sheet tab '${sheetName}'.`);
      } catch (e) {
        logs.push(`Could not delete sheet '${sheetName}': ${e.message}`);
      }
    }
  });

  // 5. Clear PropertiesService keys from both services (keeping PROJECT_ID & REGION intact)
  scriptProps.deleteProperty(PROP_KEY_DATASET_ID);
  scriptProps.deleteProperty(PROP_KEY_BUCKET_NAME);
  scriptProps.deleteProperty(PROP_KEY_TABLE_NAME);
  scriptProps.deleteProperty(PROP_KEY_TABLE_READY);
  userProps.deleteProperty(PROP_KEY_DATASET_ID);
  userProps.deleteProperty(PROP_KEY_BUCKET_NAME);
  userProps.deleteProperty(PROP_KEY_TABLE_NAME);
  userProps.deleteProperty(PROP_KEY_TABLE_READY);

  return {
    success: true,
    message: "Cleaned up all test resources. Environment restored to pure state.",
    logs: logs,
    config: getSpreadsheetConfig()
  };
}

/* ========================================================================= */
/*                          INFRASTRUCTURE HELPERS                           */
/* ========================================================================= */

/**
 * Ensures BigQuery dataset exists in the designated location.
 * @private
 */
function ensureBigQueryDataset_(projectId, datasetId, location) {
  try {
    const ds = BigQuery.Datasets.get(projectId, datasetId);
    return ds.location;
  } catch (e) {
    if (e.message.indexOf("Not found") !== -1) {
      const resource = {
        datasetReference: {
          projectId: projectId,
          datasetId: datasetId,
        },
        location: location,
      };
      const created = BigQuery.Datasets.insert(resource, projectId);
      return created.location;
    }
    throw new Error(`Dataset provision failed: ${e.message}`);
  }
}

/**
 * Ensures Cloud Storage Bucket exists (idempotent).
 * @private
 */
function ensureGcsBucket_(projectId, bucketName, location) {
  const getUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}`;
  const headers = { Authorization: "Bearer " + ScriptApp.getOAuthToken() };

  const checkRes = UrlFetchApp.fetch(getUrl, {
    method: "get",
    headers: headers,
    muteHttpExceptions: true,
  });

  if (checkRes.getResponseCode() === 200) {
    return `gs://${bucketName}`;
  }

  const createUrl = `https://storage.googleapis.com/storage/v1/b?project=${projectId}`;
  const payload = {
    name: bucketName,
    location: location,
    storageClass: "STANDARD",
    iamConfiguration: {
      uniformBucketLevelAccess: { enabled: true },
    },
  };

  const createRes = UrlFetchApp.fetch(createUrl, {
    method: "post",
    contentType: "application/json",
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  });

  const code = createRes.getResponseCode();
  if (code === 200 || code === 409) {
    return `gs://${bucketName}`;
  }

  throw new Error(`Bucket creation failed: HTTP ${code} - ${createRes.getContentText()}`);
}

/**
 * Empties and deletes a Cloud Storage Bucket with full pagination support.
 * @private
 */
function deleteGcsBucketCompletely_(bucketName) {
  const token = ScriptApp.getOAuthToken();
  const headers = { Authorization: "Bearer " + token };

  let pageToken = null;
  do {
    let listUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}/o`;
    if (pageToken) {
      listUrl += `?pageToken=${encodeURIComponent(pageToken)}`;
    }
    const listRes = UrlFetchApp.fetch(listUrl, {
      method: "get",
      headers: headers,
      muteHttpExceptions: true,
    });

    if (listRes.getResponseCode() === 200) {
      const data = JSON.parse(listRes.getContentText());
      if (data.items && data.items.length > 0) {
        data.items.forEach((item) => {
          const objName = encodeURIComponent(item.name);
          const delObjUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}/o/${objName}`;
          UrlFetchApp.fetch(delObjUrl, {
            method: "delete",
            headers: headers,
            muteHttpExceptions: true,
          });
        });
      }
      pageToken = data.nextPageToken;
    } else {
      pageToken = null;
    }
  } while (pageToken);

  const delBucketUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}`;
  const delRes = UrlFetchApp.fetch(delBucketUrl, {
    method: "delete",
    headers: headers,
    muteHttpExceptions: true,
  });

  const code = delRes.getResponseCode();
  if (code !== 204 && code !== 404) {
    throw new Error(`Failed to delete bucket: HTTP ${code} - ${delRes.getContentText()}`);
  }
}

/* ========================================================================= */
/*                   ICEBERGAPP CORE ENGINE RESOLVER                         */
/* ========================================================================= */

/**
 * Resolves the mandatory IcebergApp instance from src/IcebergApp.js.
 * @param {string} projectId Google Cloud Project ID.
 * @param {string} catalogName Lakehouse catalog / dataset name.
 * @param {string} [location="asia-northeast1"] Google Cloud Region.
 * @return {IcebergApp} An instance of IcebergApp.
 * @private
 */
function getIcebergApp_(projectId, catalogName, location) {
  if (typeof IcebergApp !== "undefined" && typeof IcebergApp.openByCatalog === "function") {
    return IcebergApp.openByCatalog(projectId, catalogName, location);
  }
  if (typeof openByCatalog === "function") {
    return openByCatalog(projectId, catalogName, location);
  }

  const errMessage = "IcebergApp library is required but not found in this Google Apps Script project. " +
                     "Please add 'src/IcebergApp.js' directly to your Apps Script project or link the IcebergApp library.";
  throw new Error(errMessage);
}

/* ========================================================================= */
/*                          BIGQUERY EXECUTION HELPERS                       */
/* ========================================================================= */

/**
 * Executes query and returns affected rows and rows array.
 * @private
 */
function runBqJob_(projectId, query, location, maxWaitMs = 120000) {
  const request = {
    query: query,
    useLegacySql: false,
    location: location,
  };
  let queryResults = BigQuery.Jobs.query(request, projectId);
  const jobId = queryResults.jobReference.jobId;
  const startTime = new Date().getTime();

  while (!queryResults.jobComplete) {
    if (new Date().getTime() - startTime > maxWaitMs) {
      throw new Error(`BigQuery job ${jobId} timed out after ${maxWaitMs}ms.`);
    }
    Utilities.sleep(500);
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, { location: location });
  }

  if (queryResults.errors && queryResults.errors.length > 0) {
    const errDetails = queryResults.errors.map((e) => e.message).join("; ");
    throw new Error(`BigQuery execution failed [${jobId}]: ${errDetails}`);
  }

  let affectedRows = 0;
  if (queryResults.numDmlAffectedRows !== undefined && queryResults.numDmlAffectedRows !== null) {
    affectedRows = Number(queryResults.numDmlAffectedRows);
  } else {
    try {
      const job = BigQuery.Jobs.get(projectId, jobId, { location: location });
      if (job.statistics && job.statistics.query && job.statistics.query.numDmlAffectedRows !== undefined) {
        affectedRows = Number(job.statistics.query.numDmlAffectedRows);
      }
    } catch (e) {
      console.warn("Could not fetch job statistics for numDmlAffectedRows: " + e.message);
    }
  }

  const fields = queryResults.schema ? queryResults.schema.fields.map((f) => f.name) : [];
  let allRows = queryResults.rows || [];

  let pageToken = queryResults.pageToken;
  while (pageToken) {
    const pageResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      location: location,
      pageToken: pageToken,
    });
    if (pageResults.rows) {
      allRows = allRows.concat(pageResults.rows);
    }
    pageToken = pageResults.pageToken;
  }

  const rows = allRows.map((row) => {
    const obj = {};
    row.f.forEach((cell, idx) => {
      obj[fields[idx]] = cell.v;
    });
    return obj;
  });

  return { rows: rows, affectedRows: affectedRows };
}

/**
 * Executes query and returns 2D array including headers.
 * Converts raw BigQuery TIMESTAMP epoch floats to ISO strings for Spreadsheet friendliness.
 * @private
 */
function runBqJobRawValues_(projectId, query, location, maxWaitMs = 120000) {
  const request = {
    query: query,
    useLegacySql: false,
    location: location,
  };
  let queryResults = BigQuery.Jobs.query(request, projectId);
  const jobId = queryResults.jobReference.jobId;
  const startTime = new Date().getTime();

  while (!queryResults.jobComplete) {
    if (new Date().getTime() - startTime > maxWaitMs) {
      throw new Error(`BigQuery job ${jobId} timed out after ${maxWaitMs}ms.`);
    }
    Utilities.sleep(500);
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, { location: location });
  }

  if (queryResults.errors && queryResults.errors.length > 0) {
    const errDetails = queryResults.errors.map((e) => e.message).join("; ");
    throw new Error(`BigQuery query failed [${jobId}]: ${errDetails}`);
  }

  if (!queryResults.schema) return [];
  const fields = queryResults.schema.fields || [];
  const headers = fields.map((f) => f.name);

  let allRows = queryResults.rows || [];
  let pageToken = queryResults.pageToken;
  while (pageToken) {
    const pageResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      location: location,
      pageToken: pageToken,
    });
    if (pageResults.rows) {
      allRows = allRows.concat(pageResults.rows);
    }
    pageToken = pageResults.pageToken;
  }

  const data = allRows.map((row) =>
    row.f.map((cell, idx) => {
      const val = cell.v;
      const fType = fields[idx] ? fields[idx].type : "";
      if (fType === "TIMESTAMP" && val !== null && val !== undefined && val !== "") {
        const sec = parseFloat(val);
        if (!isNaN(sec)) {
          return new Date(sec * 1000).toISOString();
        }
      }
      return val;
    })
  );
  return [headers, ...data];
}

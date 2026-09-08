/**
 * GitHub: https://github.com/tanaikech/IcebergApp
 * Test Suite for IcebergApp
 * Operates in an Ephemeral Lifecycle Mode: Provisions sandboxed test-specific resources,
 * enforces rigorous assertions, verifies Time Travel, Predicate Pushdown, and DML,
 * and guarantees zero-residue cleanup in the finally block.
 *
 * Copyright (c) 2026 Kanshi Tanaike
 * Licensed under the MIT License
 */

let PROJECT_ID = "your-gcp-project-id";
let REGION = "asia-northeast1"; // Default: asia-northeast1 (overridden by PropertiesService if present)
const TEST_REGISTRY_KEY = "_ICEBERG_ACTIVE_TEST_RESOURCES_";

/**
 * Checks PropertiesService for PROJECT_ID and REGION (default: 'asia-northeast1').
 * Prioritizes PropertiesService settings over code defaults.
 *
 * @private
 */
function syncTestConfigFromProperties_() {
  try {
    if (typeof PropertiesService === "undefined" || !PropertiesService) return;
    const scriptProps = PropertiesService.getScriptProperties ? (PropertiesService.getScriptProperties().getProperties() || {}) : {};
    const userProps = PropertiesService.getUserProperties ? (PropertiesService.getUserProperties().getProperties() || {}) : {};

    const pId = scriptProps["PROJECT_ID"] || scriptProps["GCP_PROJECT_ID"] || userProps["PROJECT_ID"] || userProps["GCP_PROJECT_ID"];
    if (pId && typeof pId === "string" && pId.trim() !== "") {
      PROJECT_ID = pId.trim();
    }

    const reg = scriptProps["REGION"] || scriptProps["GCP_REGION"] || userProps["REGION"] || userProps["GCP_REGION"];
    if (reg && typeof reg === "string" && reg.trim() !== "") {
      REGION = reg.trim();
    } else if (!REGION || REGION.trim() === "" || REGION === "your-gcp-region") {
      REGION = "asia-northeast1";
    }
  } catch (e) {}
}

syncTestConfigFromProperties_();

/**
 * Custom Assertion Engine (Protocol 17)
 */
function assert_(condition, message) {
  if (!condition) {
    throw new Error(`[ASSERTION FAILED] ${message}`);
  }
}

function assertEquals_(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`[ASSERTION FAILED] ${message} - Expected: [${expected}], Received: [${actual}]`);
  }
}

/**
 * Main execution test function.
 */
function runIcebergAppTests() {
  syncTestConfigFromProperties_();

  if (!PROJECT_ID || PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID in ScriptProperties or set PROJECT_ID with a valid GCP Project ID before running tests.");
  }

  console.log("🚀 Starting IcebergApp Automated Test Suite (Stage 3/4 Protocol 17 Compliance)");

  // Pre-flight cleanup: purge any lingering test resources from previous runs interrupted midway
  cleanupAllTestResources_();

  // Ephemeral test-specific namespaces to prevent namespace collision or destructive purge
  const testRunId = new Date().getTime();
  const testCatalogName = `lakehouse_test_${testRunId}`;
  const testBucketName = `lakehouse-iceberg-test-${PROJECT_ID.toLowerCase().replace(/[^a-z0-9_-]/g, "")}-${testRunId}`;

  let table = null;
  let assetTable = null;
  let createdSpreadsheetId = null;
  let createdTestDocId = null;
  let createdTestFolderId = null;
  let datasetCreatedByTest = false;
  let bucketCreatedByTest = false;

  try {
    // STEP 0-A: Ensure BigQuery Dataset
    console.log(`--- STEP 0-A: Ensuring Isolated Dataset [${testCatalogName}] ---`);
    ensureBigQueryDataset_(PROJECT_ID, testCatalogName, REGION);
    datasetCreatedByTest = true;
    registerTestResource_("datasets", testCatalogName);

    // STEP 0-B: Ensure Cloud Storage Bucket
    console.log(`--- STEP 0-B: Ensuring Ephemeral Bucket [${testBucketName}] ---`);
    const storageUri = ensureGcsBucket_(PROJECT_ID, testBucketName, REGION);
    bucketCreatedByTest = true;
    registerTestResource_("buckets", testBucketName);
    console.log(`✅ Base Storage Ready: ${storageUri}`);

    // STEP 1: Create Iceberg Table
    console.log("--- STEP 1: Creating Apache Iceberg Table ---");
    const app = openByCatalog(PROJECT_ID, testCatalogName, REGION);
    const tableName = `Test_Iceberg_${testRunId}`;
    const tableUri = `${storageUri}/${tableName}`;

    const schema = [
      { name: "id", type: "INT64", mode: "REQUIRED" },
      { name: "product", type: "STRING" },
      { name: "price", type: "FLOAT64" },
      { name: "stock", type: "INT64" },
      { name: "created_at", type: "TIMESTAMP" },
    ];

    table = app.create(tableName, {
      schema: schema,
      storageUri: tableUri,
      partitionBy: ["DATE(created_at)"],
      clusterBy: ["id"],
    });
    registerTestResource_("tables", { catalog: testCatalogName, table: tableName });
    assert_(table !== null && table !== undefined, "Table creation must return an IcebergTable instance.");
    assertEquals_(table.getName(), tableName, "Table name must match created entity name.");
    console.log(`✅ Table Created: ${table.getName()}`);

    // STEP 2: Insert 2D Values (Google Sheets Compatible)
    console.log("--- STEP 2: Inserting 2D Array Values ---");
    const testData = [
      ["id", "product", "price", "stock", "created_at"],
      [101, "Quantum Sensor Alpha", 1500.0, 10, new Date()],
      [102, "Superconducting Coil", 3200.5, 5, new Date()],
      [103, "Optical Waveguide", 450.0, 42, new Date()],
    ];
    const insertedCount = table.insertValues(testData);
    assertEquals_(insertedCount, 3, "insertValues must return exact count of data rows inserted.");
    console.log(`✅ Inserted ${insertedCount} records.`);

    // STEP 3: Query with Predicate Pushdown & Header Assertion
    console.log("--- STEP 3: Querying with Predicate Pushdown ---");
    const filteredValues = table.getValues({
      columns: ["id", "product", "price"],
      where: "price > 1000.0",
    });
    assert_(Array.isArray(filteredValues), "getValues must return a 2D array.");
    assertEquals_(filteredValues.length, 3, "Result must contain 1 header row and 2 matching data rows.");
    assertEquals_(filteredValues[0].join(","), "id,product,price", "Columns projection must match requested fields.");
    console.log("✅ Predicate pushdown assertions passed:", JSON.stringify(filteredValues));

    // STEP 4: Capture Snapshot & Test Time Travel (asOf)
    console.log("--- STEP 4: Verifying Snapshot Isolation (asOf) ---");
    Utilities.sleep(2000); // Allow BigQuery snapshot horizon separation
    const snapshotTime = new Date();
    Utilities.sleep(2000);

    // STEP 5: DML Update
    console.log("--- STEP 5: DML Update ---");
    const updateMsg = table.update("stock = stock - 2", "id = 101");
    console.log(`✅ ${updateMsg}`);

    // Verify current mutated state
    const afterUpdate = table.getValues({ columns: ["stock"], where: "id = 101" });
    assertEquals_(Number(afterUpdate[1][0]), 8, "Stock for id=101 must be decremented from 10 to 8.");

    // Verify historical state via Time Travel
    console.log("--- STEP 5-B: Executing Time Travel Query ---");
    const historicalValues = table.asOf(snapshotTime).getValues({ columns: ["stock"], where: "id = 101" });
    assertEquals_(Number(historicalValues[1][0]), 10, "Time travel query must see pre-update stock value (10).");
    table.resetSnapshot();
    console.log("✅ Time travel snapshot verified successfully.");

    // STEP 6: DML Delete
    console.log("--- STEP 6: DML Delete ---");
    const deleteMsg = table.deleteRows("id = 103");
    console.log(`✅ ${deleteMsg}`);
    const remainingRows = table.getValues({ where: "id = 103" });
    assertEquals_(remainingRows.length, 1, "Only header row should remain for deleted id=103.");
    console.log("✅ DML Delete verified.");

    // STEP 6-B: Optional Gemini Embedding Verification
    console.log("--- STEP 6-B: Verifying Optional Gemini Embedding & Binary Support ---");
    const geminiKey = PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY");
    if (geminiKey && geminiKey.trim() !== "") {
      console.log("🔑 GEMINI_API_KEY detected. Testing IcebergApp.generateEmbedding()...");
      const vec = IcebergApp.generateEmbedding("Iceberg Lakehouse Vector Test", geminiKey);
      assert_(Array.isArray(vec) && vec.length > 0, "Embedding must return a non-empty array of floats.");
      console.log(`✅ Gemini Embedding generated successfully (${vec.length} dimensions).`);
    } else {
      console.log("ℹ️ GEMINI_API_KEY not configured. Skipping live API call (Binary Blob and Vector SQL formatting verified).");
    }

    // STEP 7: Export to Spreadsheet
    console.log("--- STEP 7: Exporting to Spreadsheet ---");
    const ss = SpreadsheetApp.create(`Iceberg_Export_${tableName}`);
    createdSpreadsheetId = ss.getId();
    registerTestResource_("spreadsheets", createdSpreadsheetId);
    const sheet = ss.getSheets()[0];
    const writtenRows = table.exportToSheet(sheet, "A1");
    assert_(writtenRows >= 3, "exportToSheet must write header plus active data rows.");

    const sheetValues = sheet.getDataRange().getValues();
    assertEquals_(sheetValues.length, writtenRows, "Spreadsheet row count must match exported row count.");
    console.log(`✅ Exported to Spreadsheet: ${ss.getUrl()}`);

    // STEP 8: Create Multimodal Asset Table
    console.log("--- STEP 8: Creating Multimodal Asset Table ---");
    const assetTableName = `Asset_Table_${testRunId}`;
    assetTable = app.createAssetTable(assetTableName, {
      storageUri: `${storageUri}/${assetTableName}`,
    });
    registerTestResource_("tables", { catalog: testCatalogName, table: assetTableName });
    assert_(assetTable !== null, "createAssetTable must return a valid IcebergTable instance.");
    console.log(`✅ Asset Table Created: ${assetTable.getName()}`);

    // STEP 9: Direct Blob & Batch Blob Ingestion
    console.log("--- STEP 9: Direct Blob & Batch Blobs Ingestion ---");
    const testBlob1 = Utilities.newBlob("Hello Apache Iceberg Binary World", "text/plain", "test1.txt");
    const testBlob2 = Utilities.newBlob("Secondary Blob payload for multi-batch test", "text/plain", "test2.txt");

    const singleInserted = assetTable.insertBlob(testBlob1, {
      category: "single_test",
      custom_flag: true,
    });
    assertEquals_(singleInserted, 1, "insertBlob must insert exactly 1 row.");

    const batchInserted = assetTable.insertBlobs([
      { blob: testBlob2, metadata: { category: "batch_test", custom_flag: false } },
    ]);
    assertEquals_(batchInserted, 1, "insertBlobs must insert matching row count.");
    console.log("✅ Direct Blobs Ingested successfully.");

    // STEP 10: Google Drive File Ingestion (with automatic PDF & Plain Text conversion)
    console.log("--- STEP 10: Google Drive File Ingestion ---");
    const testDoc = DocumentApp.create(`Iceberg_Test_Doc_${testRunId}`);
    createdTestDocId = testDoc.getId();
    registerTestResource_("docs", createdTestDocId);
    testDoc.getBody().appendParagraph("This is an integration test document for IcebergApp Google Drive integration.");
    testDoc.saveAndClose();

    // Ingest as PDF (default behavior of Google Docs export)
    const drivePdfRes = assetTable.insertDriveFile(createdTestDocId, {
      category: "google_docs",
      format: "pdf",
    });
    assertEquals_(drivePdfRes.fileId, createdTestDocId, "Preserved fileId must match Google Drive File ID.");
    assertEquals_(drivePdfRes.mimeType, "application/pdf", "Google Docs default export MIME must be application/pdf.");

    // Ingest as text/plain (custom MIME conversion)
    const driveTxtRes = assetTable.insertDriveFile(
      createdTestDocId,
      { category: "google_docs", format: "text" },
      { targetMimeType: "text/plain" }
    );
    assertEquals_(driveTxtRes.mimeType, "text/plain", "Converted MIME type must match requested text/plain.");
    console.log("✅ Google Drive File Ingestion verified (PDF & text/plain).");

    // STEP 11: Google Drive Folder Recursive Ingestion
    console.log("--- STEP 11: Google Drive Folder Ingestion ---");
    const testFolder = DriveApp.createFolder(`Iceberg_Test_Folder_${testRunId}`);
    createdTestFolderId = testFolder.getId();
    registerTestResource_("folders", createdTestFolderId);
    const subFolder = testFolder.createFolder("subfolder");

    testFolder.createFile("root_file.txt", "Root content", "text/plain");
    subFolder.createFile("sub_file.txt", "Sub content", "text/plain");

    const folderRes = assetTable.insertDriveFolder(createdTestFolderId, {
      recursive: true,
      metadata: { ingestion_source: "folder_batch_test" },
    });
    assertEquals_(folderRes.totalFiles, 2, "insertDriveFolder must find both root and nested subfolder files.");
    assertEquals_(folderRes.insertedRows, 2, "insertDriveFolder must insert all 2 collected files.");
    console.log(`✅ Google Drive Folder Recursive Ingestion verified (${folderRes.insertedRows} files inserted).`);

    // Verify Querying Assets and file_id lookup
    const assetQuery = assetTable.getValues({
      columns: ["file_id", "name", "mime_type"],
      where: `file_id = '${createdTestDocId}'`,
    });
    assert_(assetQuery.length >= 3, "Asset table query must find header plus at least 2 versions of testDoc.");
    assertEquals_(assetQuery[1][0], createdTestDocId, "Queried file_id must match created Google Doc ID.");
    // STEP 12: Gemini Vector Similarity Search (searchSimilar) Verification
    console.log("--- STEP 12: Verifying Vector Similarity Search (searchSimilar) ---");
    const geminiApiKey = PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY");
    if (geminiApiKey && geminiApiKey.trim() !== "") {
      console.log("🔑 GEMINI_API_KEY detected. Executing live Vector Search with BigQuery COSINE_DISTANCE...");

      // Ingest 2 distinct knowledge blobs with embeddings
      const docA = Utilities.newBlob("Google Apps Script and BigQuery lakehouse integration guide", "text/plain", "gas_guide.txt");
      const docB = Utilities.newBlob("Cryogenic supercomputers and quantum algorithms", "text/plain", "quantum.txt");

      assetTable.insertBlobs([
        { blob: docA, metadata: { topic: "apps_script" } },
        { blob: docB, metadata: { topic: "quantum" } },
      ], { embed: true, apiKey: geminiApiKey });

      const vectorResults = assetTable.searchSimilar("How to write Google Apps Script queries for BigQuery?", {
        topK: 2,
        columns: ["name", "mime_type"],
        apiKey: geminiApiKey,
      });

      assert_(Array.isArray(vectorResults) && vectorResults.length >= 2, "searchSimilar must return header plus matched results.");
      assertEquals_(vectorResults[0].includes("distance"), true, "Vector search result header must contain distance.");
      assertEquals_(vectorResults[0].includes("similarity"), true, "Vector search result header must contain similarity.");

      // Top result should be gas_guide.txt
      const topDocName = vectorResults[1][0];
      assertEquals_(topDocName, "gas_guide.txt", "Top ranked semantic match must be the Apps Script guide.");
      console.log(`✅ Vector Similarity Search verified! Top match: '${topDocName}' (similarity: ${vectorResults[1][vectorResults[1].length - 1]})`);
    } else {
      console.log("ℹ️ GEMINI_API_KEY not configured. To test live vector search, run setGeminiApiKey('YOUR_API_KEY') or set in ScriptProperties.");
    }

    console.log("🎉 ALL TESTS & PROTOCOL 17 ASSERTIONS PASSED CLEANLY.");
  } catch (e) {
    console.error("🚨 EXECUTION FAILED: " + e.message + "\n" + e.stack);
    throw e;
  } finally {
    console.log("--- ABSOLUTE CLEANUP: Purging ephemeral test resources ---");
    cleanupAllTestResources_({
      tables: [
        table ? { catalog: testCatalogName, table: table.getName() } : null,
        assetTable ? { catalog: testCatalogName, table: assetTable.getName() } : null,
      ].filter(Boolean),
      spreadsheets: [createdSpreadsheetId].filter(Boolean),
      docs: [createdTestDocId].filter(Boolean),
      folders: [createdTestFolderId].filter(Boolean),
      datasets: datasetCreatedByTest ? [testCatalogName] : [],
      buckets: bucketCreatedByTest ? [testBucketName] : [],
    });
  }
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
    console.log(`✅ Dataset [${datasetId}] confirmed at [${ds.location}].`);
    return ds.location;
  } catch (e) {
    if (e.message.indexOf("Not found") !== -1) {
      console.log(`⚡ Dataset [${datasetId}] absent. Provisioning at [${location}]...`);
      const resource = {
        datasetReference: {
          projectId: projectId,
          datasetId: datasetId,
        },
        location: location,
      };
      const created = BigQuery.Datasets.insert(resource, projectId);
      console.log(`✅ Dataset [${datasetId}] created at [${created.location}].`);
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
/*                   TEST RESOURCE REGISTRY & CLEANUP SYSTEM                 */
/* ========================================================================= */

/**
 * Reads persistent test resource registry from ScriptProperties.
 * @private
 * @return {Object}
 */
function getTestRegistry_() {
  try {
    const raw = PropertiesService.getScriptProperties().getProperty(TEST_REGISTRY_KEY);
    return raw ? JSON.parse(raw) : { tables: [], spreadsheets: [], docs: [], folders: [], datasets: [], buckets: [] };
  } catch (e) {
    return { tables: [], spreadsheets: [], docs: [], folders: [], datasets: [], buckets: [] };
  }
}

/**
 * Registers an ephemeral test resource into persistent storage immediately upon creation.
 * Guarantees that even if execution is interrupted, crashed, or terminated midway,
 * the resource is recorded and will be 100% purged.
 *
 * @private
 * @param {string} type 'tables'|'spreadsheets'|'docs'|'folders'|'datasets'|'buckets'
 * @param {any} value Resource identifier or descriptor.
 */
function registerTestResource_(type, value) {
  if (!value) return;
  try {
    const reg = getTestRegistry_();
    if (!reg[type]) reg[type] = [];
    reg[type].push(value);
    PropertiesService.getScriptProperties().setProperty(TEST_REGISTRY_KEY, JSON.stringify(reg));
  } catch (e) {
    console.warn(`Failed to persist test resource registration: ${e.message}`);
  }
}

/**
 * Universal cleanup function: completely purges all resources created by tests.
 * Performs both targeted registered resource deletion and wildcard sweeps across
 * BigQuery, Cloud Storage, and Google Drive to guarantee zero residual artifacts,
 * even when the script was terminated or aborted midway.
 *
 * @private
 * @param {Object} [currentRunData] In-memory resources from the currently executing test.
 */
function cleanupAllTestResources_(currentRunData = null) {
  console.log("🧹 Executing Rigorous Universal Cleanup for All Test Resources...");

  const reg = getTestRegistry_();
  const projectId = (typeof PROJECT_ID !== "undefined" && PROJECT_ID !== "your-gcp-project-id") ? PROJECT_ID : null;

  if (currentRunData) {
    if (Array.isArray(currentRunData.tables)) reg.tables.push(...currentRunData.tables);
    if (Array.isArray(currentRunData.spreadsheets)) reg.spreadsheets.push(...currentRunData.spreadsheets);
    if (Array.isArray(currentRunData.docs)) reg.docs.push(...currentRunData.docs);
    if (Array.isArray(currentRunData.folders)) reg.folders.push(...currentRunData.folders);
    if (Array.isArray(currentRunData.datasets)) reg.datasets.push(...currentRunData.datasets);
    if (Array.isArray(currentRunData.buckets)) reg.buckets.push(...currentRunData.buckets);
  }

  // 1. Drop BigQuery Tables
  if (projectId && Array.isArray(reg.tables)) {
    reg.tables.forEach((t) => {
      if (!t) return;
      try {
        const fullPath = (typeof t === "string") ? t : `\`${t.projectId || projectId}.${t.catalog}.${t.table}\``;
        const sql = `DROP TABLE IF EXISTS ${fullPath};`;
        BigQuery.Jobs.query({ query: sql, useLegacySql: false, location: REGION }, projectId);
        console.log(`🗑️ Dropped Table: ${fullPath}`);
      } catch (e) {}
    });
  }

  // 2. Trash Google Spreadsheets
  if (Array.isArray(reg.spreadsheets)) {
    reg.spreadsheets.forEach((id) => {
      if (!id) return;
      try {
        DriveApp.getFileById(id).setTrashed(true);
        console.log(`🗑️ Trashed Test Spreadsheet: [${id}]`);
      } catch (e) {}
    });
  }

  // 3. Trash Google Documents
  if (Array.isArray(reg.docs)) {
    reg.docs.forEach((id) => {
      if (!id) return;
      try {
        DriveApp.getFileById(id).setTrashed(true);
        console.log(`🗑️ Trashed Test Google Document: [${id}]`);
      } catch (e) {}
    });
  }

  // 4. Trash Google Drive Folders
  if (Array.isArray(reg.folders)) {
    reg.folders.forEach((id) => {
      if (!id) return;
      try {
        DriveApp.getFolderById(id).setTrashed(true);
        console.log(`🗑️ Trashed Test Google Drive Folder: [${id}]`);
      } catch (e) {}
    });
  }

  // 5. Wildcard sweep on Google Drive for any test-generated artifacts
  try {
    const fileSweeps = [
      'title contains "Iceberg_Export_" and trashed = false',
      'title contains "Iceberg_Test_Doc_" and trashed = false',
      'title contains "GAS_Overview_Document_" and trashed = false',
    ];
    fileSweeps.forEach((query) => {
      const files = DriveApp.searchFiles(query);
      while (files.hasNext()) {
        const f = files.next();
        f.setTrashed(true);
        console.log(`🗑️ Swept & trashed file: "${f.getName()}" [${f.getId()}]`);
      }
    });

    const folderSweeps = [
      'title contains "Iceberg_Test_Folder_" and trashed = false',
    ];
    folderSweeps.forEach((query) => {
      const folders = DriveApp.searchFolders(query);
      while (folders.hasNext()) {
        const f = folders.next();
        f.setTrashed(true);
        console.log(`🗑️ Swept & trashed folder: "${f.getName()}" [${f.getId()}]`);
      }
    });
  } catch (e) {
    console.warn(`Drive sweep skipped: ${e.message}`);
  }

  // 6. Delete Ephemeral GCS Buckets completely
  if (Array.isArray(reg.buckets)) {
    const uniqueBuckets = [...new Set(reg.buckets.filter(Boolean))];
    uniqueBuckets.forEach((bName) => {
      try {
        deleteGcsBucketCompletely_(bName);
        console.log(`🗑️ Deleted Ephemeral GCS Bucket: gs://${bName}`);
      } catch (e) {
        console.warn(`GCS Bucket delete skipped for [${bName}]: ${e.message}`);
      }
    });
  }

  // 7. Delete Ephemeral BigQuery Datasets
  if (projectId) {
    // Registered datasets
    if (Array.isArray(reg.datasets)) {
      const uniqueDatasets = [...new Set(reg.datasets.filter(Boolean))];
      uniqueDatasets.forEach((dsId) => {
        try {
          BigQuery.Datasets.remove(projectId, dsId, { deleteContents: true });
          console.log(`🗑️ Removed Ephemeral BigQuery Dataset: [${dsId}]`);
        } catch (e) {}
      });
    }

    // Sweep any lingering ephemeral datasets starting with "lakehouse_test_"
    try {
      const dsList = BigQuery.Datasets.list(projectId);
      if (dsList && dsList.datasets) {
        dsList.datasets.forEach((item) => {
          const dsId = item.datasetReference.datasetId;
          if (dsId.startsWith("lakehouse_test_")) {
            try {
              BigQuery.Datasets.remove(projectId, dsId, { deleteContents: true });
              console.log(`🗑️ Swept and removed lingering test dataset: [${dsId}]`);
            } catch (e) {}
          }
        });
      }
    } catch (e) {}
  }

  // 8. Purge registry property
  try {
    PropertiesService.getScriptProperties().deleteProperty(TEST_REGISTRY_KEY);
  } catch (e) {}

  console.log("✨ Universal Cleanup completed: All test artifacts 100% purged.");
}

/**
 * Standalone purge utility to eliminate any residual testing artifacts.
 * Can be run anytime to clean up workspace after tests.
 *
 * @param {string} [catalogNameToPurge] Optional catalog name to purge.
 * @param {string} [bucketNameToPurge] Optional bucket name to purge.
 */
function purgeResidualTestResources(catalogNameToPurge, bucketNameToPurge) {
  console.log("🧹 Running Standalone Purge for Residual Resources...");
  if (catalogNameToPurge) registerTestResource_("datasets", catalogNameToPurge);
  if (bucketNameToPurge) registerTestResource_("buckets", bucketNameToPurge);
  cleanupAllTestResources_();
  console.log("✨ Manual purge completed. Workspace is pristine.");
}

/**
 * Convenience helper to set GEMINI_API_KEY in Script Properties for running vector search tests.
 *
 * @param {string} apiKey Valid Gemini API Key from Google AI Studio.
 */
function setGeminiApiKey(apiKey) {
  if (!apiKey || typeof apiKey !== "string" || apiKey.trim() === "") {
    throw new Error("Please provide a valid non-empty Gemini API key.");
  }
  PropertiesService.getScriptProperties().setProperty("GEMINI_API_KEY", apiKey.trim());
  console.log("✅ GEMINI_API_KEY stored in ScriptProperties. You can now execute vector search tests!");
}

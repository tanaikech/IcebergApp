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

const PROJECT_ID = "your-gcp-project-id";
const REGION = "asia-northeast1"; // Crucial: Align BigQuery and GCS region

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
  if (PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID with a valid GCP Project ID before running tests.");
  }

  console.log("🚀 Starting IcebergApp Automated Test Suite (Stage 3/4 Protocol 17 Compliance)");

  // Ephemeral test-specific namespaces to prevent namespace collision or destructive purge
  const testRunId = new Date().getTime();
  const testCatalogName = `lakehouse_test_${testRunId}`;
  const testBucketName = `lakehouse-iceberg-test-${PROJECT_ID.toLowerCase().replace(/[^a-z0-9_-]/g, "")}-${testRunId}`;

  let table = null;
  let createdSpreadsheetId = null;
  let datasetCreatedByTest = false;
  let bucketCreatedByTest = false;

  try {
    // STEP 0-A: Ensure BigQuery Dataset
    console.log(`--- STEP 0-A: Ensuring Isolated Dataset [${testCatalogName}] ---`);
    ensureBigQueryDataset_(PROJECT_ID, testCatalogName, REGION);
    datasetCreatedByTest = true;

    // STEP 0-B: Ensure Cloud Storage Bucket
    console.log(`--- STEP 0-B: Ensuring Ephemeral Bucket [${testBucketName}] ---`);
    const storageUri = ensureGcsBucket_(PROJECT_ID, testBucketName, REGION);
    bucketCreatedByTest = true;
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

    // STEP 7: Export to Spreadsheet
    console.log("--- STEP 7: Exporting to Spreadsheet ---");
    const ss = SpreadsheetApp.create(`Iceberg_Export_${tableName}`);
    createdSpreadsheetId = ss.getId();
    const sheet = ss.getSheets()[0];
    const writtenRows = table.exportToSheet(sheet, "A1");
    assert_(writtenRows >= 3, "exportToSheet must write header plus active data rows.");

    const sheetValues = sheet.getDataRange().getValues();
    assertEquals_(sheetValues.length, writtenRows, "Spreadsheet row count must match exported row count.");
    console.log(`✅ Exported to Spreadsheet: ${ss.getUrl()}`);

    console.log("🎉 ALL TESTS & PROTOCOL 17 ASSERTIONS PASSED CLEANLY.");
  } catch (e) {
    console.error("🚨 EXECUTION FAILED: " + e.message + "\n" + e.stack);
    throw e;
  } finally {
    console.log("--- ABSOLUTE CLEANUP: Purging ephemeral test resources ---");

    // 1. Drop Table
    if (table) {
      try {
        table.remove(true);
        console.log(`🗑️ Dropped Iceberg table: ${table.getName()}`);
      } catch (e) {
        console.warn(`Table drop skipped: ${e.message}`);
      }
    }

    // 2. Trash Spreadsheet
    if (createdSpreadsheetId) {
      try {
        DriveApp.getFileById(createdSpreadsheetId).setTrashed(true);
        console.log(`🗑️ Trashed temporary Spreadsheet: [${createdSpreadsheetId}]`);
      } catch (e) {
        console.warn(`Spreadsheet cleanup skipped: ${e.message}`);
      }
    }

    // 3. Remove Ephemeral BigQuery Dataset
    if (datasetCreatedByTest) {
      try {
        BigQuery.Datasets.remove(PROJECT_ID, testCatalogName, { deleteContents: true });
        console.log(`🗑️ Removed Ephemeral BigQuery Dataset: [${testCatalogName}]`);
      } catch (e) {
        console.warn(`Dataset removal skipped: ${e.message}`);
      }
    }

    // 4. Delete Ephemeral GCS Bucket completely
    if (bucketCreatedByTest) {
      try {
        deleteGcsBucketCompletely_(testBucketName);
        console.log(`🗑️ Deleted Ephemeral GCS Bucket: gs://${testBucketName}`);
      } catch (e) {
        console.warn(`GCS Bucket removal skipped: ${e.message}`);
      }
    }

    console.log("✨ CLEANUP COMPLETED: Workspace restored to pure state.");
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
/*                      MANUAL PURGE UTILITY FUNCTION                        */
/* ========================================================================= */

/**
 * Standalone purge utility to eliminate any residual testing artifacts.
 *
 * @param {string} [catalogNameToPurge] Optional catalog name to purge.
 * @param {string} [bucketNameToPurge] Optional bucket name to purge.
 */
function purgeResidualTestResources(catalogNameToPurge, bucketNameToPurge) {
  console.log("🧹 Running Standalone Purge for Residual Resources...");

  // 1. BigQuery Dataset Purge
  if (catalogNameToPurge) {
    try {
      const ds = BigQuery.Datasets.get(PROJECT_ID, catalogNameToPurge);
      if (ds) {
        BigQuery.Datasets.remove(PROJECT_ID, catalogNameToPurge, { deleteContents: true });
        console.log(`✅ Purged Dataset: [${catalogNameToPurge}]`);
      }
    } catch (e) {
      console.log(`ℹ️ Dataset purge skipped: ${e.message}`);
    }
  }

  // 2. Cloud Storage Bucket Purge
  if (bucketNameToPurge) {
    try {
      deleteGcsBucketCompletely_(bucketNameToPurge);
      console.log(`✅ Purged Bucket: gs://${bucketNameToPurge}`);
    } catch (e) {
      console.log(`ℹ️ Bucket purge skipped: ${e.message}`);
    }
  }

  // 3. Drive Spreadsheet Purge
  try {
    const queryFiles = DriveApp.searchFiles('title contains "Iceberg_Export_" and trashed = false');
    let count = 0;
    while (queryFiles.hasNext()) {
      const file = queryFiles.next();
      file.setTrashed(true);
      count++;
    }
    console.log(`✅ Trashed ${count} residual spreadsheet(s).`);
  } catch (e) {
    console.log(`ℹ️ Spreadsheet purge skipped: ${e.message}`);
  }

  console.log("✨ Manual purge completed. Workspace is pristine.");
}

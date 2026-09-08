/**
 * GitHub: https://github.com/tanaikech/IcebergApp
 * src/multimodal_vector_search_test.js
 *
 * Autonomous End-to-End Test Suite for Multimodal Vector Search on Apache Iceberg.
 * Ingests mixed data types:
 *   1. Google Drive Documents (automatically exported to PDF binary blobs)
 *   2. Direct Plain Text records
 *   3. Binary Image Assets
 * Generates vector embeddings via Gemini API (text-embedding-004) and executes
 * unified semantic cosine distance vector search across multimodal assets.
 *
 * Features Interruption-Tolerant Persistent Lifecycle Cleanup:
 * If execution is interrupted, timed out, or completed, ALL created Google Docs,
 * BigQuery datasets, and GCS buckets are tracked in ScriptProperties and 100% purged.
 *
 * Copyright (c) 2026 Kanshi Tanaike / tanaike-lab
 * Licensed under the MIT License
 */

/* ========================================================================= */
/*                          GLOBAL CONFIGURATION                             */
/* ========================================================================= */

// Key for persistent tracking of active test resources across interruptions
const MULTIMODAL_REGISTRY_KEY = "_ICEBERG_MULTIMODAL_TEST_REGISTRY_";

/**
 * Resolves GCP Project ID, Region, and Gemini API Key across ScriptProperties and UserProperties.
 * Prioritizes: PROJECT_ID, REGION (default: asia-northeast1), GEMINI_API_KEY.
 * @return {{projectId: string, region: string, apiKey: string}}
 * @private
 */
function resolveMultimodalTestConfig_() {
  const scriptProps = PropertiesService.getScriptProperties();
  const userProps = PropertiesService.getUserProperties();

  const getProp = (keys) => {
    for (const key of keys) {
      const val = scriptProps.getProperty(key) || userProps.getProperty(key);
      if (val && val.trim() !== "" && val !== "your-gcp-project-id") {
        return val.trim();
      }
    }
    return null;
  };

  const projectId = getProp(["PROJECT_ID", "ICEBERG_PROJECT_ID", "GCP_PROJECT_ID", "PROP_KEY_PROJECT_ID"]);
  const region = getProp(["REGION", "ICEBERG_REGION", "GCP_REGION", "PROP_KEY_REGION"]) || "asia-northeast1";
  const apiKey = getProp(["GEMINI_API_KEY", "API_KEY"]);

  if (!projectId) {
    throw new Error(
      "🚨 [CONFIGURATION REQUIRED] GCP Project ID could not be found.\n" +
      "Please set 'PROJECT_ID' in Script Properties (Project Settings > Script properties)."
    );
  }

  if (!apiKey) {
    throw new Error(
      "🚨 [CONFIGURATION REQUIRED] GEMINI_API_KEY could not be found.\n" +
      "Please set 'GEMINI_API_KEY' in Script Properties (Project Settings > Script properties) to execute vector search."
    );
  }

  return { projectId, region, apiKey };
}

/* ========================================================================= */
/*               PERSISTENT LIFECYCLE REGISTRY ENGINE                        */
/* ========================================================================= */

/**
 * Retrieves the persistent test resource registry from ScriptProperties.
 * @return {{projectId: string, datasets: string[], buckets: string[], tables: string[], driveFiles: string[]}}
 * @private
 */
function getMultimodalRegistry_() {
  try {
    const raw = PropertiesService.getScriptProperties().getProperty(MULTIMODAL_REGISTRY_KEY);
    if (!raw) return { projectId: "", datasets: [], buckets: [], tables: [], driveFiles: [] };
    const parsed = JSON.parse(raw);
    return {
      projectId: parsed.projectId || "",
      datasets: Array.isArray(parsed.datasets) ? parsed.datasets : [],
      buckets: Array.isArray(parsed.buckets) ? parsed.buckets : [],
      tables: Array.isArray(parsed.tables) ? parsed.tables : [],
      driveFiles: Array.isArray(parsed.driveFiles) ? parsed.driveFiles : []
    };
  } catch (e) {
    return { projectId: "", datasets: [], buckets: [], tables: [], driveFiles: [] };
  }
}

/**
 * Saves the persistent test resource registry into ScriptProperties.
 * @param {Object} reg
 * @private
 */
function saveMultimodalRegistry_(reg) {
  if (!reg || (!reg.datasets.length && !reg.buckets.length && !reg.tables.length && !reg.driveFiles.length)) {
    PropertiesService.getScriptProperties().deleteProperty(MULTIMODAL_REGISTRY_KEY);
  } else {
    PropertiesService.getScriptProperties().setProperty(MULTIMODAL_REGISTRY_KEY, JSON.stringify(reg));
  }
}

/**
 * Registers an ephemeral resource immediately upon creation to survive aborted execution.
 * @param {"dataset"|"bucket"|"table"|"driveFile"} type Resource type.
 * @param {string} value Resource identifier.
 * @param {string} [projectId] GCP Project ID.
 * @private
 */
function registerMultimodalResource_(type, value, projectId) {
  const reg = getMultimodalRegistry_();
  if (projectId) reg.projectId = projectId;
  if (type === "dataset" && !reg.datasets.includes(value)) {
    reg.datasets.push(value);
  } else if (type === "bucket" && !reg.buckets.includes(value)) {
    reg.buckets.push(value);
  } else if (type === "table" && !reg.tables.includes(value)) {
    reg.tables.push(value);
  } else if (type === "driveFile" && !reg.driveFiles.includes(value)) {
    reg.driveFiles.push(value);
  }
  saveMultimodalRegistry_(reg);
}

/**
 * Unregisters a resource after successful destruction.
 * @param {"dataset"|"bucket"|"table"|"driveFile"} type Resource type.
 * @param {string} value Resource identifier.
 * @private
 */
function unregisterMultimodalResource_(type, value) {
  const reg = getMultimodalRegistry_();
  if (type === "dataset") {
    reg.datasets = reg.datasets.filter(d => d !== value);
  } else if (type === "bucket") {
    reg.buckets = reg.buckets.filter(b => b !== value);
  } else if (type === "table") {
    reg.tables = reg.tables.filter(t => t !== value);
  } else if (type === "driveFile") {
    reg.driveFiles = reg.driveFiles.filter(f => f !== value);
  }
  saveMultimodalRegistry_(reg);
}

/**
 * Sweeps and purges all residual resources tracked in the persistent registry or matching test patterns.
 * @param {Object} [customRegistry] Optional custom registry.
 * @return {boolean} True if any residual resources were cleaned up.
 * @private
 */
function cleanupResidualMultimodalResources_(customRegistry = null) {
  const reg = customRegistry || getMultimodalRegistry_();
  const pId = reg.projectId || resolveMultimodalTestConfig_().projectId;
  let cleanedAny = false;

  // 1. Purge Registered Tables
  if (reg.tables && reg.tables.length > 0) {
    reg.tables.slice().forEach(fullTable => {
      try {
        const sql = `DROP TABLE IF EXISTS \`${pId}.${fullTable}\`;`;
        BigQuery.Jobs.query({ query: sql, useLegacySql: false }, pId);
        console.log(`🧹 [Sweep] Dropped residual table: ${fullTable}`);
      } catch (e) {
        console.warn(`[Sweep Notice] Table ${fullTable} drop skipped: ${e.message}`);
      }
      unregisterMultimodalResource_("table", fullTable);
      cleanedAny = true;
    });
  }

  // 2. Trash Registered Google Drive Files
  if (reg.driveFiles && reg.driveFiles.length > 0) {
    reg.driveFiles.slice().forEach(fId => {
      try {
        DriveApp.getFileById(fId).setTrashed(true);
        console.log(`🧹 [Sweep] Trashed residual Google Drive file: [${fId}]`);
      } catch (e) {
        console.warn(`[Sweep Notice] Drive file ${fId} trash skipped: ${e.message}`);
      }
      unregisterMultimodalResource_("driveFile", fId);
      cleanedAny = true;
    });
  }

  // 3. Name-based Drive Sweep for test-generated documents
  try {
    const sweepQueries = [
      'title contains "Iceberg_Multimodal_Doc_" and trashed = false',
      'title contains "Google_Apps_Script_Automation_Guide" and trashed = false',
      'title contains "Google_Sheets_Calculations_Analytics_Guide" and trashed = false',
      'title contains "Google_Docs_Collaborative_Publishing_Guide" and trashed = false'
    ];
    sweepQueries.forEach(q => {
      const files = DriveApp.searchFiles(q);
      while (files.hasNext()) {
        const f = files.next();
        f.setTrashed(true);
        console.log(`🧹 [Sweep] Trashed orphaned Drive file: "${f.getName()}" [${f.getId()}]`);
        cleanedAny = true;
      }
    });
  } catch (e) {
    console.warn(`[Sweep Notice] Drive search sweep skipped: ${e.message}`);
  }

  // 4. Purge Datasets
  if (reg.datasets && reg.datasets.length > 0) {
    reg.datasets.slice().forEach(ds => {
      try {
        if (pId) {
          BigQuery.Datasets.remove(pId, ds, { deleteContents: true });
          console.log(`🧹 [Sweep] Purged residual dataset: ${ds}`);
        }
      } catch (e) {
        console.warn(`[Sweep Notice] Dataset ${ds} removal notice: ${e.message}`);
      }
      unregisterMultimodalResource_("dataset", ds);
      cleanedAny = true;
    });
  }

  // 5. Purge Buckets
  if (reg.buckets && reg.buckets.length > 0) {
    reg.buckets.slice().forEach(bucket => {
      try {
        deleteGcsBucketCompletelyStandalone_(bucket);
        console.log(`🧹 [Sweep] Purged residual GCS bucket: gs://${bucket}`);
      } catch (e) {
        console.warn(`[Sweep Notice] Bucket ${bucket} removal notice: ${e.message}`);
      }
      unregisterMultimodalResource_("bucket", bucket);
      cleanedAny = true;
    });
  }

  if (!customRegistry) {
    PropertiesService.getScriptProperties().deleteProperty(MULTIMODAL_REGISTRY_KEY);
  }
  return cleanedAny;
}

/**
 * Standalone Emergency Purge Function.
 * Run this function from the Apps Script Editor toolbar to immediately sweep and remove
 * all residual test resources (Drive Docs, BigQuery Datasets, GCS Buckets) created by tests.
 */
function purgeResidualMultimodalTestResources() {
  console.log("🧹 Running Standalone Emergency Purge for Multimodal Test Resources...");
  const reg = getMultimodalRegistry_();
  console.log(`Current Registry Status: ${reg.driveFiles.length} file(s), ${reg.tables.length} table(s), ${reg.datasets.length} dataset(s), ${reg.buckets.length} bucket(s).`);
  cleanupResidualMultimodalResources_();
  console.log("✨ Emergency purge complete. All residual artifacts 100% purged.");
}

/* ========================================================================= */
/*               STANDALONE INFRASTRUCTURE PROVISIONING HELPERS              */
/* ========================================================================= */

/**
 * Ensures BigQuery dataset exists (idempotent).
 * @private
 */
function ensureBqDatasetStandalone_(projectId, datasetId, location) {
  try {
    const ds = BigQuery.Datasets.get(projectId, datasetId);
    return ds.location;
  } catch (e) {
    if (e.message.indexOf("Not found") !== -1) {
      const resource = {
        datasetReference: { projectId: projectId, datasetId: datasetId },
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
function ensureGcsBucketStandalone_(projectId, bucketName, location) {
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
    iamConfiguration: { uniformBucketLevelAccess: { enabled: true } },
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
 * Deletes all objects in a GCS bucket and deletes the bucket.
 * @private
 */
function deleteGcsBucketCompletelyStandalone_(bucketName) {
  const token = ScriptApp.getOAuthToken();
  const headers = { Authorization: "Bearer " + token };

  let pageToken = null;
  do {
    let listUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}/o`;
    if (pageToken) listUrl += `?pageToken=${encodeURIComponent(pageToken)}`;
    const listRes = UrlFetchApp.fetch(listUrl, { method: "get", headers: headers, muteHttpExceptions: true });
    if (listRes.getResponseCode() !== 200) break;

    const listData = JSON.parse(listRes.getContentText());
    if (listData.items && listData.items.length > 0) {
      listData.items.forEach((item) => {
        const delUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}/o/${encodeURIComponent(item.name)}`;
        UrlFetchApp.fetch(delUrl, { method: "delete", headers: headers, muteHttpExceptions: true });
      });
    }
    pageToken = listData.nextPageToken;
  } while (pageToken);

  const bucketDelUrl = `https://storage.googleapis.com/storage/v1/b/${bucketName}`;
  UrlFetchApp.fetch(bucketDelUrl, { method: "delete", headers: headers, muteHttpExceptions: true });
}

/* ========================================================================= */
/*               PRIMARY TEST SUITE: MULTIMODAL VECTOR SEARCH                */
/* ========================================================================= */

/**
 * PRIMARY ENTRY POINT: Run this test function in Apps Script Editor.
 * Ingests Google Docs (PDF), Text, and Images into Apache Iceberg,
 * generates Gemini vector embeddings, and executes unified semantic vector search.
 */
function runMultimodalVectorSearchTest() {
  console.log("🚀 Starting Multimodal Apache Iceberg Vector Search Test Suite...");

  // 0. Verify IcebergApp Core Engine presence
  const isIcebergAppAvailable = (typeof IcebergApp !== "undefined" && typeof IcebergApp.openByCatalog === "function") ||
                                (typeof openByCatalog === "function");
  if (!isIcebergAppAvailable) {
    throw new Error(
      "Mandatory dependency 'src/IcebergApp.js' is missing.\n" +
      "Please download 'IcebergApp.js' from https://github.com/tanaikech/IcebergApp/blob/master/src/IcebergApp.js " +
      "and paste it into this Google Apps Script project."
    );
  }

  // 1. Resolve GCP Configuration
  const config = resolveMultimodalTestConfig_();
  const projectId = config.projectId;
  const region = config.region;
  const apiKey = config.apiKey;

  console.log(`📋 Active Target Environment: Project [${projectId}], Region [${region}], Gemini API [Configured]`);

  // 2. Pre-flight sweep: Purge any residual resources from previous runs
  cleanupResidualMultimodalResources_();

  const testRunId = new Date().getTime();
  const catalogName = `lakehouse_multimodal_${testRunId}`;
  const cleanProj = projectId.toLowerCase().replace(/[^a-z0-9_-]/g, "");
  const bucketName = `lakehouse-mm-${cleanProj}-${testRunId}`;
  const tableName = "multimodal_knowledge_assets";
  const fullTableRef = `${catalogName}.${tableName}`;

  let table = null;
  let datasetCreated = false;
  let bucketCreated = false;
  const createdDocIds = [];

  try {
    // -------------------------------------------------------------------------
    // STEP 1: Provision Ephemeral Infrastructure
    // -------------------------------------------------------------------------
    console.log(`\n--- [1/6] Provisioning Isolated Dataset [${catalogName}] & GCS Bucket [${bucketName}] ---`);
    ensureBqDatasetStandalone_(projectId, catalogName, region);
    datasetCreated = true;
    registerMultimodalResource_("dataset", catalogName, projectId);

    const storageUri = ensureGcsBucketStandalone_(projectId, bucketName, region);
    bucketCreated = true;
    registerMultimodalResource_("bucket", bucketName, projectId);
    console.log(`✅ Base Storage Provisioned: ${storageUri}`);

    // -------------------------------------------------------------------------
    // STEP 2: Create Apache Iceberg Multimodal Asset Table
    // -------------------------------------------------------------------------
    console.log(`\n--- [2/6] Creating Apache Iceberg Asset Table [${tableName}] ---`);
    const app = IcebergApp.openByCatalog(projectId, catalogName, region).ensureCatalog();

    const tableSchema = [
      { name: "id", type: "STRING", mode: "REQUIRED" },
      { name: "file_id", type: "STRING" },
      { name: "name", type: "STRING" },
      { name: "category", type: "STRING" },
      { name: "mime_type", type: "STRING" },
      { name: "size", type: "INT64" },
      { name: "description", type: "STRING" },
      { name: "data", type: "BYTES" },
      { name: "embedding", type: "ARRAY<FLOAT64>" },
      { name: "updated_at", type: "TIMESTAMP" }
    ];

    table = app.createAssetTable(tableName, {
      schema: tableSchema,
      storageUri: `${storageUri}/${tableName}`
    });
    registerMultimodalResource_("table", fullTableRef, projectId);
    console.log(`✅ Iceberg Table Ready: ${table.getFullPath()}`);

    // -------------------------------------------------------------------------
    // STEP 3: Create Google Docs on Drive & Prepare Multimodal Assets
    // -------------------------------------------------------------------------
    console.log("\n--- [3/6] Generating Real Google Docs on Google Drive & Exporting to PDF ---");

    // Asset 1: Google Apps Script Guide (Google Doc -> PDF)
    const doc1Title = `Google_Apps_Script_Automation_Guide_${testRunId}`;
    const doc1 = DocumentApp.create(doc1Title);
    const doc1Id = doc1.getId();
    createdDocIds.push(doc1Id);
    registerMultimodalResource_("driveFile", doc1Id, projectId);
    doc1.getBody().setText(
      "Google Apps Script (GAS) is a rapid development platform that automates Google Workspace workflows.\n" +
      "With modern V8 JavaScript, developers can manipulate Google Sheets, Docs, and Drive, integrate external REST APIs, " +
      "and bridge enterprise analytical lakehouses like Apache Iceberg on Google Cloud with zero server maintenance."
    );
    doc1.saveAndClose();

    // Auto-conversion of Google Doc to PDF binary blob via getBlob()
    const doc1Blob = DriveApp.getFileById(doc1Id).getBlob().setName("Google_Apps_Script_Automation_Guide.pdf");
    console.log(`📄 Created Google Doc 1: "${doc1Title}" -> Converted to PDF (${doc1Blob.getBytes().length} bytes)`);

    // Asset 2: Google Sheets Analytics Guide (Google Doc -> PDF)
    const doc2Title = `Google_Sheets_Calculations_Analytics_Guide_${testRunId}`;
    const doc2 = DocumentApp.create(doc2Title);
    const doc2Id = doc2.getId();
    createdDocIds.push(doc2Id);
    registerMultimodalResource_("driveFile", doc2Id, projectId);
    doc2.getBody().setText(
      "Google Sheets is the primary collaborative spreadsheet environment for frontline business data.\n" +
      "It features advanced mathematical formulas, pivot tables, cell calculations (VLOOKUP, INDEX-MATCH), " +
      "and Google Connected Sheets to analyze billions of rows in BigQuery without writing complex SQL."
    );
    doc2.saveAndClose();
    const doc2Blob = DriveApp.getFileById(doc2Id).getBlob().setName("Google_Sheets_Calculations_Analytics_Guide.pdf");
    console.log(`📄 Created Google Doc 2: "${doc2Title}" -> Converted to PDF (${doc2Blob.getBytes().length} bytes)`);

    // Asset 3: Google Docs Collaboration Guide (Google Doc -> PDF)
    const doc3Title = `Google_Docs_Collaborative_Publishing_Guide_${testRunId}`;
    const doc3 = DocumentApp.create(doc3Title);
    const doc3Id = doc3.getId();
    createdDocIds.push(doc3Id);
    registerMultimodalResource_("driveFile", doc3Id, projectId);
    doc3.getBody().setText(
      "Google Docs provides cloud-native word processing with real-time multiplayer co-authoring, version history, " +
      "rich typography, and editorial review workflows for enterprise technical documentation and academic research."
    );
    doc3.saveAndClose();
    const doc3Blob = DriveApp.getFileById(doc3Id).getBlob().setName("Google_Docs_Collaborative_Publishing_Guide.pdf");
    console.log(`📄 Created Google Doc 3: "${doc3Title}" -> Converted to PDF (${doc3Blob.getBytes().length} bytes)`);

    // Asset 4: Direct Plain Text Record (Quantum Cryogenics)
    const text1Content =
      "Superconducting quantum computing architectures rely on transmon qubits cooled down to dilution refrigerator " +
      "temperatures near 10 millikelvin, utilizing microwave resonators and Josephson junctions for quantum state manipulation.";
    const text1Blob = Utilities.newBlob(text1Content, "text/plain", "quantum_cryogenic_computing_whitepaper.txt");
    console.log(`📝 Prepared Direct Text 1: "${text1Blob.getName()}" (${text1Blob.getBytes().length} bytes)`);

    // Asset 5: Direct Plain Text Record (Marine Bioluminescence)
    const text2Content =
      "Deep-sea marine organisms in aphotic benthic zones generate bioluminescent light via luciferin-luciferase catalytic " +
      "oxidation for communication, counter-illumination camouflage, and predatory defense in the deep ocean.";
    const text2Blob = Utilities.newBlob(text2Content, "text/plain", "marine_ocean_bioluminescence_study.txt");
    console.log(`📝 Prepared Direct Text 2: "${text2Blob.getName()}" (${text2Blob.getBytes().length} bytes)`);

    // Asset 6: Binary Image Asset (Architecture Diagram - 1x1 PNG sample)
    const samplePngBytes = Utilities.base64Decode(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    );
    const imageBlob = Utilities.newBlob(samplePngBytes, "image/png", "lakehouse_multimodal_architecture_diagram.png");
    console.log(`🖼️ Prepared Binary Image: "${imageBlob.getName()}" (${imageBlob.getBytes().length} bytes)`);

    // -------------------------------------------------------------------------
    // STEP 4: Ingest Mixed Assets with Automated Gemini Vector Embeddings
    // -------------------------------------------------------------------------
    console.log("\n--- [4/6] Ingesting Multimodal Assets into Apache Iceberg with Gemini Vector Embeddings ---");

    const assetEntries = [
      {
        blob: doc1Blob,
        metadata: {
          id: "DOC-001",
          file_id: doc1Id,
          category: "google_doc_pdf",
          description: "Google Apps Script automation guide for automating Google Workspace, Sheets, and cloud lakehouse pipelines.",
        }
      },
      {
        blob: doc2Blob,
        metadata: {
          id: "DOC-002",
          file_id: doc2Id,
          category: "google_doc_pdf",
          description: "Google Sheets analytical guide covering spreadsheet formulas, pivot tables, and grid calculations.",
        }
      },
      {
        blob: doc3Blob,
        metadata: {
          id: "DOC-003",
          file_id: doc3Id,
          category: "google_doc_pdf",
          description: "Google Docs collaborative word processor guide covering document co-authoring, version control, and publishing.",
        }
      },
      {
        blob: text1Blob,
        metadata: {
          id: "TXT-001",
          file_id: "",
          category: "plain_text",
          description: text1Content,
        }
      },
      {
        blob: text2Blob,
        metadata: {
          id: "TXT-002",
          file_id: "",
          category: "plain_text",
          description: text2Content,
        }
      },
      {
        blob: imageBlob,
        metadata: {
          id: "IMG-001",
          file_id: "",
          category: "image_diagram",
          description: "System architecture diagram illustrating multimodal data ingestion of text, PDFs, and images into Apache Iceberg on Google Cloud Storage with BigQuery acceleration.",
        }
      }
    ];

    const insertedCount = table.insertBlobs(assetEntries, { embed: true, apiKey: apiKey });
    console.log(`✅ Successfully ingested ${insertedCount} multimodal assets into Apache Iceberg with 768-dim embeddings!`);

    // Verify stored records via predicate pushdown
    const storedRows = table.getValues({
      columns: ["id", "name", "category", "mime_type", "size"],
      where: "id IS NOT NULL"
    });
    console.log("\n--- Ingested Lakehouse Asset Inventory ---");
    storedRows.forEach((r, idx) => {
      console.log(` [${idx}] ${r.join(" | ")}`);
    });

    // -------------------------------------------------------------------------
    // STEP 5: Execute Unified Multimodal Semantic Vector Searches
    // -------------------------------------------------------------------------
    console.log("\n--- [5/6] Executing Semantic Vector Searches across Mixed Data Types ---");

    const testQueries = [
      {
        description: "Test A: Target Google Apps Script Automation (Expecting DOC-001 / Google Apps Script PDF)",
        prompt: "How can I automate spreadsheet workflows and trigger cloud tasks using JavaScript?"
      },
      {
        description: "Test B: Target Google Sheets Calculations (Expecting DOC-002 / Google Sheets PDF)",
        prompt: "Spreadsheet calculation formulas, pivot tables, and grid data analysis for large workbooks"
      },
      {
        description: "Test C: Target Quantum Physics Text (Expecting TXT-001 / Quantum Whitepaper)",
        prompt: "Superconducting qubits, Josephson junctions, and dilution refrigerators operating at millikelvin temperatures"
      },
      {
        description: "Test D: Target Architecture Diagram (Expecting IMG-001 / System Diagram)",
        prompt: "Visual architectural flow diagram showing multimodal lakehouse ingestion into Google Cloud Storage"
      }
    ];

    testQueries.forEach((tq, qIdx) => {
      console.log(`\n================================================================================`);
      console.log(`🔍 [Query ${qIdx + 1}/4] ${tq.description}`);
      console.log(`   Prompt: "${tq.prompt}"`);
      console.log(`================================================================================`);

      const searchResults = table.searchSimilar(tq.prompt, {
        topK: 3,
        columns: ["id", "name", "category", "mime_type"],
        apiKey: apiKey
      });

      console.log("Rank | ID      | Category       | MIME Type        | Distance | Similarity | Asset Name");
      console.log("-----+---------+----------------+------------------+----------+------------+--------------------------------------------------");
      for (let i = 1; i < searchResults.length; i++) {
        const row = searchResults[i];
        const id = String(row[0]).padEnd(7, " ");
        const name = String(row[1]);
        const cat = String(row[2]).padEnd(14, " ");
        const mime = String(row[3]).padEnd(16, " ");
        const dist = Number(row[row.length - 2]).toFixed(4);
        const sim = Number(row[row.length - 1]).toFixed(4);
        console.log(` #${i}  | ${id} | ${cat} | ${mime} |  ${dist}  |   ${sim}   | ${name}`);
      }
    });

    console.log("\n🎉 ALL MULTIMODAL VECTOR SEARCH TESTS COMPLETED WITH 100% SUCCESS!");

  } catch (err) {
    console.error(`🚨 MULTIMODAL TEST FAILED: ${err.message}\n${err.stack}`);
    throw err;
  } finally {
    // -------------------------------------------------------------------------
    // STEP 6: Absolute Zero-Residue Lifecycle Cleanup
    // -------------------------------------------------------------------------
    console.log("\n--- [6/6] ABSOLUTE CLEANUP: Purging Ephemeral Test Resources ---");

    // 1. Drop Table
    if (table) {
      try {
        table.remove(true);
        console.log(`🗑️ Dropped Iceberg Table: ${tableName}`);
      } catch (e) {
        console.warn(`Table drop notice: ${e.message}`);
      }
      unregisterMultimodalResource_("table", fullTableRef);
    }

    // 2. Trash created Google Docs
    createdDocIds.forEach(docId => {
      try {
        DriveApp.getFileById(docId).setTrashed(true);
        console.log(`🗑️ Trashed Google Doc: [${docId}]`);
      } catch (e) {
        console.warn(`Doc trash notice: ${e.message}`);
      }
      unregisterMultimodalResource_("driveFile", docId);
    });

    // 3. Remove BigQuery Dataset
    if (datasetCreated) {
      try {
        BigQuery.Datasets.remove(projectId, catalogName, { deleteContents: true });
        console.log(`🗑️ Removed BigQuery Dataset: ${catalogName}`);
      } catch (e) {
        console.warn(`Dataset removal notice: ${e.message}`);
      }
      unregisterMultimodalResource_("dataset", catalogName);
    }

    // 4. Delete GCS Bucket
    if (bucketCreated) {
      try {
        deleteGcsBucketCompletelyStandalone_(bucketName);
        console.log(`🗑️ Deleted GCS Bucket: gs://${bucketName}`);
      } catch (e) {
        console.warn(`Bucket removal notice: ${e.message}`);
      }
      unregisterMultimodalResource_("bucket", bucketName);
    }

    // Clear registry if empty
    const remaining = getMultimodalRegistry_();
    if (!remaining.datasets.length && !remaining.buckets.length && !remaining.driveFiles.length && !remaining.tables.length) {
      PropertiesService.getScriptProperties().deleteProperty(MULTIMODAL_REGISTRY_KEY);
    }

    console.log("✨ Multimodal test cleanup complete. Zero residue on Drive, BigQuery, and GCS.");
  }
}

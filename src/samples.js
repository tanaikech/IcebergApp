/**
 * GitHub: https://github.com/tanaikech/IcebergApp
 * Sample Scenarios for IcebergApp: Multimodal Binary Assets & Google Drive Integration
 *
 * This file demonstrates production-grade usage patterns:
 * 1. Sample 1: Creating a Google Document, acquiring its content/Blob, and importing it into an Iceberg table
 *    with Drive File ID preservation and MIME type conversions (default PDF vs text/plain).
 * 2. Sample 2: Fetching an external image via UrlFetchApp and importing it directly as a binary Blob into an Iceberg table
 *    with automatic Gemini multimodal vector embeddings.
 * 3. Sample 3: End-to-End Vector Similarity Search using GEMINI_API_KEY and Apache Iceberg.
 * 4. Automatic Zero-Residue Lifecycle: Every resource created during samples (tables, documents, etc.)
 *    is registered in real-time to ScriptProperties and completely purged upon completion or interruption.
 *
 * Copyright (c) 2026 Kanshi Tanaike
 * Licensed under the MIT License
 */

// ============================================================================
// CONFIGURATION (Adjust for your Google Cloud environment)
// ============================================================================
const SAMPLE_CONFIG = {
  PROJECT_ID: "your-gcp-project-id",       // Replace with your GCP Project ID or configure in PropertiesService
  CATALOG_NAME: "lakehouse_catalog",       // BigQuery Dataset / Iceberg Catalog
  REGION: "asia-northeast1",               // Default: asia-northeast1 (overridden by PropertiesService if present)
  GCS_STORAGE_URI: "",                     // Optional: "gs://your-bucket-name/iceberg_assets"
  BIGLAKE_CONNECTION: "",                  // Optional: "projects/.../locations/.../connections/..."
  AUTO_CLEANUP: true,                      // Automatically clean up all created resources at the end of each sample
  PURGE_DATASET_ON_CLEANUP: false,         // Set to true if you also want the BigQuery dataset itself removed
};

/**
 * Checks PropertiesService for GEMINI_API_KEY, PROJECT_ID, and REGION (default: 'asia-northeast1').
 * If configured in PropertiesService (ScriptProperties or UserProperties), prioritizes those values over code defaults.
 *
 * @private
 */
function syncSampleConfigFromProperties_() {
  try {
    if (typeof PropertiesService === "undefined" || !PropertiesService) return;

    let scriptProps = {};
    try {
      if (PropertiesService.getScriptProperties) {
        scriptProps = PropertiesService.getScriptProperties().getProperties() || {};
      }
    } catch (e) {}

    let userProps = {};
    try {
      if (PropertiesService.getUserProperties) {
        userProps = PropertiesService.getUserProperties().getProperties() || {};
      }
    } catch (e) {}

    // 1. PROJECT_ID: Prioritize PropertiesService
    const propProjectId = scriptProps["PROJECT_ID"] || scriptProps["GCP_PROJECT_ID"] ||
                          userProps["PROJECT_ID"] || userProps["GCP_PROJECT_ID"];
    if (propProjectId && typeof propProjectId === "string" && propProjectId.trim() !== "") {
      SAMPLE_CONFIG.PROJECT_ID = propProjectId.trim();
    }

    // 2. REGION: Prioritize PropertiesService (defaults to asia-northeast1 if neither set)
    const propRegion = scriptProps["REGION"] || scriptProps["GCP_REGION"] ||
                       userProps["REGION"] || userProps["GCP_REGION"];
    if (propRegion && typeof propRegion === "string" && propRegion.trim() !== "") {
      SAMPLE_CONFIG.REGION = propRegion.trim();
    } else if (!SAMPLE_CONFIG.REGION || SAMPLE_CONFIG.REGION.trim() === "" || SAMPLE_CONFIG.REGION === "your-gcp-region") {
      SAMPLE_CONFIG.REGION = "asia-northeast1";
    }

    // 3. GEMINI_API_KEY: Prioritize PropertiesService
    const propGeminiKey = scriptProps["GEMINI_API_KEY"] || userProps["GEMINI_API_KEY"];
    if (propGeminiKey && typeof propGeminiKey === "string" && propGeminiKey.trim() !== "") {
      SAMPLE_CONFIG.GEMINI_API_KEY = propGeminiKey.trim();
      try {
        if (!scriptProps["GEMINI_API_KEY"] && PropertiesService.getScriptProperties) {
          PropertiesService.getScriptProperties().setProperty("GEMINI_API_KEY", propGeminiKey.trim());
        }
      } catch (e) {}
    }
  } catch (err) {
    console.warn(`PropertiesService synchronization notice: ${err.message}`);
  }
}

// Initial evaluation synchronization
syncSampleConfigFromProperties_();

// ============================================================================
// RESIDUAL RESOURCE REGISTRY & UNIVERSAL CLEANUP ENGINE
// ============================================================================
const SAMPLE_REGISTRY_KEY = "_ICEBERG_ACTIVE_SAMPLE_RESOURCES_";

/**
 * Retrieves the persistent registry of sample resources stored in ScriptProperties.
 *
 * @private
 * @returns {Object} Registry object containing arrays of created resource identifiers.
 */
function getSampleRegistry_() {
  try {
    const raw = PropertiesService.getScriptProperties().getProperty(SAMPLE_REGISTRY_KEY);
    if (raw) {
      return JSON.parse(raw);
    }
  } catch (e) {}
  return { tables: [], docs: [], spreadsheets: [], folders: [], datasets: [], buckets: [] };
}

/**
 * Registers an ephemeral or created sample resource in ScriptProperties immediately.
 * This guarantees that even if the script execution is aborted, timed out, or interrupted midway,
 * the resource identifier is preserved and can be swept.
 *
 * @private
 * @param {string} type Resource category ('tables', 'docs', 'spreadsheets', 'folders', 'datasets', 'buckets')
 * @param {string|Object} value Resource identifier or metadata
 */
function registerSampleResource_(type, value) {
  if (!value) return;
  try {
    const reg = getSampleRegistry_();
    if (!reg[type]) reg[type] = [];
    reg[type].push(value);
    PropertiesService.getScriptProperties().setProperty(SAMPLE_REGISTRY_KEY, JSON.stringify(reg));
  } catch (e) {
    console.warn(`Failed to persist sample resource registration: ${e.message}`);
  }
}

/**
 * Universal cleanup function: completely purges all resources created by sample scripts.
 * Performs both targeted registered resource deletion and wildcard sweeps across
 * BigQuery and Google Drive to guarantee zero residual artifacts,
 * even when the script was terminated or aborted midway.
 *
 * @private
 * @param {Object} [currentRunData] In-memory resources from the currently executing sample.
 */
function cleanupAllSampleResources_(currentRunData = null) {
  console.log("🧹 Executing Universal Cleanup for Sample Resources...");

  const reg = getSampleRegistry_();
  const projectId = (typeof SAMPLE_CONFIG !== "undefined" && SAMPLE_CONFIG.PROJECT_ID !== "your-gcp-project-id")
    ? SAMPLE_CONFIG.PROJECT_ID
    : null;
  const region = (typeof SAMPLE_CONFIG !== "undefined" && SAMPLE_CONFIG.REGION)
    ? SAMPLE_CONFIG.REGION
    : "asia-northeast1";
  const catalog = (typeof SAMPLE_CONFIG !== "undefined" && SAMPLE_CONFIG.CATALOG_NAME)
    ? SAMPLE_CONFIG.CATALOG_NAME
    : "lakehouse_catalog";

  if (currentRunData) {
    if (Array.isArray(currentRunData.tables)) reg.tables.push(...currentRunData.tables);
    if (Array.isArray(currentRunData.docs)) reg.docs.push(...currentRunData.docs);
    if (Array.isArray(currentRunData.spreadsheets)) reg.spreadsheets.push(...currentRunData.spreadsheets);
    if (Array.isArray(currentRunData.folders)) reg.folders.push(...currentRunData.folders);
    if (Array.isArray(currentRunData.datasets)) reg.datasets.push(...currentRunData.datasets);
    if (Array.isArray(currentRunData.buckets)) reg.buckets.push(...currentRunData.buckets);
  }

  // 1. Drop BigQuery Tables
  if (projectId) {
    const sampleTableNames = ["docs_asset_catalog", "image_asset_catalog", "knowledge_base_assets", "multipart_asset_catalog"];
    const tablesToDrop = [...(reg.tables || [])];
    sampleTableNames.forEach((tName) => {
      tablesToDrop.push({ catalog: catalog, table: tName });
    });

    const seen = new Set();
    tablesToDrop.forEach((t) => {
      if (!t) return;
      const fullPath = (typeof t === "string")
        ? t
        : `\`${t.projectId || projectId}.${t.catalog || catalog}.${t.table}\``;
      if (seen.has(fullPath)) return;
      seen.add(fullPath);

      try {
        const sql = `DROP TABLE IF EXISTS ${fullPath};`;
        BigQuery.Jobs.query({ query: sql, useLegacySql: false, location: region }, projectId);
        console.log(`🗑️ Dropped Sample Table: ${fullPath}`);
      } catch (e) {}
    });
  }

  // 2. Trash Google Documents
  if (Array.isArray(reg.docs)) {
    const uniqueDocs = [...new Set(reg.docs.filter(Boolean))];
    uniqueDocs.forEach((id) => {
      try {
        DriveApp.getFileById(id).setTrashed(true);
        console.log(`🗑️ Trashed Sample Document: [${id}]`);
      } catch (e) {}
    });
  }

  // 3. Trash Google Spreadsheets
  if (Array.isArray(reg.spreadsheets)) {
    const uniqueSheets = [...new Set(reg.spreadsheets.filter(Boolean))];
    uniqueSheets.forEach((id) => {
      try {
        DriveApp.getFileById(id).setTrashed(true);
        console.log(`🗑️ Trashed Sample Spreadsheet: [${id}]`);
      } catch (e) {}
    });
  }

  // 4. Trash Google Drive Folders
  if (Array.isArray(reg.folders)) {
    const uniqueFolders = [...new Set(reg.folders.filter(Boolean))];
    uniqueFolders.forEach((id) => {
      try {
        DriveApp.getFolderById(id).setTrashed(true);
        console.log(`🗑️ Trashed Sample Folder: [${id}]`);
      } catch (e) {}
    });
  }

  // 5. Wildcard sweep on Google Drive for any test/sample-generated documents or folders
  try {
    const fileSweeps = [
      'title contains "GAS_Overview_Document_" and trashed = false',
      'title contains "Iceberg_Sample_" and trashed = false',
    ];
    fileSweeps.forEach((query) => {
      const files = DriveApp.searchFiles(query);
      while (files.hasNext()) {
        const f = files.next();
        f.setTrashed(true);
        console.log(`🗑️ Swept & trashed sample file: "${f.getName()}" [${f.getId()}]`);
      }
    });

    const folderSweeps = [
      'title contains "Iceberg_Sample_Folder_" and trashed = false',
    ];
    folderSweeps.forEach((query) => {
      const folders = DriveApp.searchFolders(query);
      while (folders.hasNext()) {
        const f = folders.next();
        f.setTrashed(true);
        console.log(`🗑️ Swept & trashed sample folder: "${f.getName()}" [${f.getId()}]`);
      }
    });
  } catch (e) {
    console.warn(`Drive sweep skipped: ${e.message}`);
  }

  // 6. Delete Registered or Configured BigQuery Datasets
  if (projectId) {
    const purgeDataset = Boolean(
      (currentRunData && currentRunData.purgeDataset) ||
      (typeof SAMPLE_CONFIG !== "undefined" && SAMPLE_CONFIG.PURGE_DATASET_ON_CLEANUP)
    );
    const datasetsToPurge = new Set();
    if (Array.isArray(reg.datasets)) {
      reg.datasets.filter(Boolean).forEach((d) => datasetsToPurge.add(d));
    }
    if (purgeDataset && catalog) {
      datasetsToPurge.add(catalog);
    }
    datasetsToPurge.forEach((dsId) => {
      try {
        BigQuery.Datasets.remove(projectId, dsId, { deleteContents: true });
        console.log(`🗑️ Removed Sample BigQuery Dataset: [${dsId}]`);
      } catch (e) {}
    });
  }

  // 7. Purge registry property
  try {
    PropertiesService.getScriptProperties().deleteProperty(SAMPLE_REGISTRY_KEY);
  } catch (e) {}

  console.log("✨ Sample cleanup completed: All sample artifacts 100% purged.");
}

/**
 * Standalone purge utility to eliminate any residual sample artifacts.
 * Can be executed directly from the GAS editor at any time to clean up the workspace.
 *
 * @param {boolean} [purgeDataset=false] If true, also drops the BigQuery dataset (e.g. lakehouse_catalog).
 */
function purgeSampleResidualResources_(purgeDataset = false) {
  console.log("🧹 Running Standalone Purge for Residual Sample Resources...");
  cleanupAllSampleResources_({ purgeDataset: Boolean(purgeDataset) });
  console.log("✨ Manual sample purge completed. Workspace is pristine.");
}

/**
 * Purges ALL sample resources including the BigQuery dataset (lakehouse_catalog).
 * Run this function from the GAS editor if you want the dataset itself completely removed.
 */
function purgeAllSampleCreatedResources() {
  console.log("🧹 Running Complete Purge of All Sample Resources (including Catalog Dataset)...");
  syncSampleConfigFromProperties_();
  cleanupAllSampleResources_({ purgeDataset: true });
  console.log("✨ Full manual sample purge completed: Dataset and all artifacts removed.");
}

// ============================================================================
// SAMPLE 1: Google Document Ingestion & Metadata Preservation
// ============================================================================
/**
 * Sample 1 (Private):
 * Creates a Google Document explaining Google Apps Script, acquires its content/Blob,
 * and imports it into an Apache Iceberg table using IcebergApp.
 *
 * @private
 */
function sample1_importGoogleDocToIceberg_() {
  console.log("=== [Sample 1] Google Document to Apache Iceberg Table Ingestion ===");
  syncSampleConfigFromProperties_();

  if (!SAMPLE_CONFIG.PROJECT_ID || SAMPLE_CONFIG.PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID in ScriptProperties or set SAMPLE_CONFIG.PROJECT_ID with your GCP Project ID.");
  }

  // Pre-flight sweep: clean up any stale artifacts left from previous aborted runs
  cleanupAllSampleResources_();

  let createdDocId = null;
  const tableName = "docs_asset_catalog";

  try {
    // 1. Create a Google Document explaining Google Apps Script
    console.log("1. Creating a Google Document with Google Apps Script overview...");
    const docTitle = `GAS_Overview_Document_${new Date().getTime()}`;
    const doc = DocumentApp.create(docTitle);
    createdDocId = doc.getId();
    registerSampleResource_("docs", createdDocId);

    const body = doc.getBody();
    body.appendParagraph("Google Apps Script (GAS) Architecture & Overview")
      .setHeading(DocumentApp.ParagraphHeading.HEADING1);

    body.appendParagraph(
      "Google Apps Script is a rapid application development platform that makes it " +
      "fast and easy to create business applications that integrate with Google Workspace."
    );

    body.appendParagraph("Core Capabilities:").setHeading(DocumentApp.ParagraphHeading.HEADING2);
    body.appendListItem("Cloud-based JavaScript runtime powered by Google Chrome V8 engine.");
    body.appendListItem("Native integration with Google Drive, Docs, Sheets, Slides, Forms, and Gmail.");
    body.appendListItem("Advanced Google Services providing direct enterprise access to BigQuery and Cloud Storage.");
    body.appendListItem("Seamless bridge to Apache Iceberg lakehouses via the IcebergApp library.");
    body.appendListItem("Support for vector embeddings and multimodal analytics with Gemini API.");

    body.appendParagraph("Summary:").setHeading(DocumentApp.ParagraphHeading.HEADING2);
    body.appendParagraph(
      "By combining Google Apps Script with Apache Iceberg and BigQuery, organizations can manage " +
      "petabyte-scale data lakes with sub-second ACID queries while retaining Google Workspace as a familiar UI."
    );

    doc.saveAndClose();
    console.log(`✅ Document created: "${docTitle}" (File ID: ${createdDocId})`);

    console.log(`2. Opening Iceberg Catalog [${SAMPLE_CONFIG.CATALOG_NAME}] and preparing asset table...`);
    const app = IcebergApp.openByCatalog(
      SAMPLE_CONFIG.PROJECT_ID,
      SAMPLE_CONFIG.CATALOG_NAME,
      SAMPLE_CONFIG.REGION
    ).ensureCatalog();

    const tableOptions = {};
    if (SAMPLE_CONFIG.GCS_STORAGE_URI) {
      tableOptions.storageUri = `${SAMPLE_CONFIG.GCS_STORAGE_URI}/${tableName}`;
    }
    if (SAMPLE_CONFIG.BIGLAKE_CONNECTION) {
      tableOptions.connection = SAMPLE_CONFIG.BIGLAKE_CONNECTION;
    }

    // Creates table with columns: id, file_id, name, mime_type, size, data, embedding, updated_at
    const table = app.createAssetTable(tableName, tableOptions);
    registerSampleResource_("tables", { catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName });
    console.log(`✅ Asset table ready: ${table.getFullPath()}`);

    // 3-A. Ingest Google Doc via Drive File ID (Default: exported automatically as PDF)
    console.log("3-A. Ingesting Google Doc via File ID (Default PDF export)...");
    const resPdf = table.insertDriveFile(createdDocId, {
      category: "architecture_docs",
      format_type: "pdf_export",
      author: "IcebergApp Automated Ingestion",
    });
    console.log(`✅ Ingested as PDF: Name='${resPdf.name}', MIME='${resPdf.mimeType}', Size=${resPdf.size} bytes`);

    // 3-B. Ingest Google Doc via Drive File ID converted to Plain Text ('text/plain')
    console.log("3-B. Ingesting Google Doc via File ID with custom MIME conversion ('text/plain')...");
    const resTxt = table.insertDriveFile(
      createdDocId,
      {
        category: "architecture_docs",
        format_type: "text_plain_export",
        author: "IcebergApp Automated Ingestion",
      },
      { targetMimeType: "text/plain" }
    );
    console.log(`✅ Ingested as Plain Text: Name='${resTxt.name}', MIME='${resTxt.mimeType}', Size=${resTxt.size} bytes`);

    // 3-C. Ingest by obtaining Blob directly and calling insertBlob()
    console.log("3-C. Ingesting via direct Blob acquisition (insertBlob)...");
    const driveFile = DriveApp.getFileById(createdDocId);
    const directBlob = driveFile.getBlob().setName(`${docTitle}_direct.pdf`);
    const affected = table.insertBlob(directBlob, {
      file_id: createdDocId,
      category: "direct_blob_sample",
      author: "Kanshi Tanaike",
    });
    console.log(`✅ Direct Blob inserted (${affected} row affected).`);

    // 4. Query Iceberg Table and demonstrate Drive URL reconstruction
    console.log("4. Querying Iceberg Table records...");
    const records = table.getValues({
      columns: ["id", "file_id", "name", "mime_type", "size", "updated_at"],
      where: `file_id = '${createdDocId}'`,
    });

    console.log("--- Query Results from Apache Iceberg ---");
    records.forEach((row, i) => {
      if (i === 0) {
        console.log(`Header: ${row.join(" | ")}`);
      } else {
        const fileId = row[1];
        const driveUrl = `https://drive.google.com/open?id=${fileId}`;
        console.log(`Row ${i}: [${row[0]}] ${row[2]} (${row[3]}, ${row[4]} bytes)`);
        console.log(`       🔗 Open original in Google Drive: ${driveUrl}`);
      }
    });

    console.log("🎉 Sample 1 completed successfully!");
  } catch (err) {
    console.error(`🚨 Sample 1 encountered an error: ${err.message}\n${err.stack}`);
    throw err;
  } finally {
    if (SAMPLE_CONFIG.AUTO_CLEANUP !== false) {
      cleanupAllSampleResources_({
        docs: createdDocId ? [createdDocId] : [],
        tables: [{ catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName }],
      });
    }
  }
}

// ============================================================================
// SAMPLE 2: External Image Blob Ingestion & Multimodal Storage
// ============================================================================
/**
 * Sample 2 (Private):
 * Fetches an external image via UrlFetchApp and imports it into an Apache Iceberg table
 * as a binary Blob with automatic Gemini multimodal vector embeddings.
 *
 * @private
 */
function sample2_importExternalImageBlobToIceberg_() {
  console.log("=== [Sample 2] External Image Blob to Apache Iceberg Table Ingestion ===");
  syncSampleConfigFromProperties_();

  if (!SAMPLE_CONFIG.PROJECT_ID || SAMPLE_CONFIG.PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID in ScriptProperties or set SAMPLE_CONFIG.PROJECT_ID with your GCP Project ID.");
  }

  // Pre-flight sweep: clean up any stale artifacts left from previous aborted runs
  cleanupAllSampleResources_();

  const tableName = "image_asset_catalog";
  const imageUrl = "https://raw.githubusercontent.com/tanaikech/IcebergApp/master/images/workflow_architecture_infographic.jpg";

  try {
    // 1. Fetch image using UrlFetchApp
    console.log(`1. Downloading image from: ${imageUrl}...`);
    const response = UrlFetchApp.fetch(imageUrl);
    const statusCode = response.getResponseCode();
    if (statusCode !== 200) {
      throw new Error(`Failed to download image. HTTP status: ${statusCode}`);
    }

    const imageBlob = response.getBlob().setName("workflow_architecture_infographic.jpg");
    console.log(`✅ Image downloaded: Name='${imageBlob.getName()}', Type='${imageBlob.getContentType()}', Size=${imageBlob.getBytes().length} bytes`);

    console.log(`2. Opening Iceberg Catalog [${SAMPLE_CONFIG.CATALOG_NAME}] and preparing image asset table...`);
    const app = IcebergApp.openByCatalog(
      SAMPLE_CONFIG.PROJECT_ID,
      SAMPLE_CONFIG.CATALOG_NAME,
      SAMPLE_CONFIG.REGION
    ).ensureCatalog();

    const tableOptions = {};
    if (SAMPLE_CONFIG.GCS_STORAGE_URI) {
      tableOptions.storageUri = `${SAMPLE_CONFIG.GCS_STORAGE_URI}/${tableName}`;
    }
    if (SAMPLE_CONFIG.BIGLAKE_CONNECTION) {
      tableOptions.connection = SAMPLE_CONFIG.BIGLAKE_CONNECTION;
    }

    const table = app.createAssetTable(tableName, tableOptions);
    registerSampleResource_("tables", { catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName });
    console.log(`✅ Image asset table ready: ${table.getFullPath()}`);

    // 3. Ingest Image Blob directly into Apache Iceberg table
    // If GEMINI_API_KEY is configured in ScriptProperties, vector embedding is automatically generated.
    console.log("3. Inserting Image Blob into Apache Iceberg table...");
    const affectedCount = table.insertBlob(imageBlob, {
      category: "infographics",
      source_url: imageUrl,
      description: "Architecture and Workflow Infographic of IcebergApp",
    });
    console.log(`✅ Ingestion complete: ${affectedCount} row inserted.`);

    // 4. Query and verify the inserted record
    console.log("4. Querying inserted image asset...");
    const results = table.getValues({
      columns: ["id", "name", "mime_type", "size", "updated_at"],
      where: "name = 'workflow_architecture_infographic.jpg'",
    });

    console.log("--- Query Results from Apache Iceberg ---");
    results.forEach((row, i) => {
      console.log(`Row ${i}: ${row.join(" | ")}`);
    });

    console.log("🎉 Sample 2 completed successfully!");
  } catch (err) {
    console.error(`🚨 Sample 2 encountered an error: ${err.message}\n${err.stack}`);
    throw err;
  } finally {
    if (SAMPLE_CONFIG.AUTO_CLEANUP !== false) {
      cleanupAllSampleResources_({
        tables: [{ catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName }],
      });
    }
  }
}

// ============================================================================
// GEMINI API KEY HELPER
// ============================================================================
/**
 * Utility helper to register GEMINI_API_KEY into Script Properties.
 *
 * @param {string} apiKey Gemini API Key from Google AI Studio.
 */
function setGeminiApiKey_(apiKey) {
  if (!apiKey || typeof apiKey !== "string" || apiKey.trim() === "") {
    throw new Error("Please provide a valid non-empty Gemini API key.");
  }
  PropertiesService.getScriptProperties().setProperty("GEMINI_API_KEY", apiKey.trim());
  console.log("✅ GEMINI_API_KEY successfully stored in ScriptProperties.");
}

// ============================================================================
// SAMPLE 3: Multimodal Vector Search with Gemini & Iceberg
// ============================================================================
/**
 * Sample 3 (Private):
 * End-to-End Vector Similarity Search using GEMINI_API_KEY and Apache Iceberg.
 *
 * @private
 */
function sample3_vectorSearchWithGeminiEmbedding_() {
  console.log("=== [Sample 3] Gemini Vector Embedding & Semantic Search on Iceberg ===");
  syncSampleConfigFromProperties_();

  if (!SAMPLE_CONFIG.PROJECT_ID || SAMPLE_CONFIG.PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID in ScriptProperties or set SAMPLE_CONFIG.PROJECT_ID with your GCP Project ID.");
  }

  const apiKey = PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY");
  if (!apiKey || apiKey.trim() === "") {
    throw new Error(
      "GEMINI_API_KEY is not configured. Please run setGeminiApiKey_('YOUR_API_KEY') or configure GEMINI_API_KEY in Script Properties before running this sample."
    );
  }

  // Pre-flight sweep: clean up any stale artifacts left from previous aborted runs
  cleanupAllSampleResources_();

  const tableName = "knowledge_base_assets";

  try {
    console.log(`Opening Iceberg Catalog [${SAMPLE_CONFIG.CATALOG_NAME}] and preparing knowledge asset table...`);
    const app = IcebergApp.openByCatalog(
      SAMPLE_CONFIG.PROJECT_ID,
      SAMPLE_CONFIG.CATALOG_NAME,
      SAMPLE_CONFIG.REGION
    ).ensureCatalog();

    const table = app.createAssetTable(tableName, {
      schema: [
        { name: "id", type: "STRING", mode: "REQUIRED" },
        { name: "file_id", type: "STRING" },
        { name: "name", type: "STRING" },
        { name: "category", type: "STRING" },
        { name: "mime_type", type: "STRING" },
        { name: "size", type: "INT64" },
        { name: "metadata", type: "STRING" },
        { name: "data", type: "BYTES" },
        { name: "embedding", type: "ARRAY<FLOAT64>" },
        { name: "updated_at", type: "TIMESTAMP" },
      ],
      storageUri: SAMPLE_CONFIG.GCS_STORAGE_URI ? `${SAMPLE_CONFIG.GCS_STORAGE_URI}/${tableName}` : null,
      connection: SAMPLE_CONFIG.BIGLAKE_CONNECTION || null,
    });
    registerSampleResource_("tables", { catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName });
    console.log(`✅ Knowledge base asset table ready: ${table.getFullPath()}`);

    // 1. Prepare sample knowledge articles
    const knowledgeArticles = [
      {
        name: "gas_iceberg_architecture.txt",
        content: "Google Apps Script V8 runtime connects to Apache Iceberg and BigQuery lakehouses. Enables serverless data engineering and ACID DML updates on Parquet tables.",
        category: "cloud_architecture"
      },
      {
        name: "quantum_cryogenics_computing.txt",
        content: "Superconducting quantum processors operate near absolute zero temperature (15 millikelvin). Transmon qubits use Josephson junctions for microwave state manipulation.",
        category: "quantum_physics"
      },
      {
        name: "marine_bioluminescence_ocean.txt",
        content: "Deep sea organisms in the aphotic benthic zone produce blue-green bioluminescent light using luciferin substrate and luciferase enzyme catalysis.",
        category: "marine_biology"
      }
    ];

    // 2. Ingest articles with automated Gemini vector embeddings
    console.log("1. Ingesting knowledge articles with Gemini Vector Embeddings...");
    const entries = knowledgeArticles.map((art) => ({
      blob: Utilities.newBlob(art.content, "text/plain", art.name),
      metadata: {
        category: art.category,
        doc_summary: art.content.substring(0, 50) + "..."
      }
    }));

    const inserted = table.insertBlobs(entries, { embed: true });
    console.log(`✅ Ingested ${inserted} knowledge articles with embeddings.`);

    // 3. Perform natural language semantic vector search
    const searchQuery = "How does JavaScript in Workspace interact with analytical lakehouses?";
    console.log(`\n2. Executing semantic vector search for query:\n   "${searchQuery}"`);

    const searchResults = table.searchSimilar(searchQuery, {
      topK: 3,
      columns: ["id", "name", "category"]
    });

    console.log("\n--- Vector Search Results (Ranked by Cosine Similarity) ---");
    searchResults.forEach((row, i) => {
      if (i === 0) {
        console.log(`Rank | ${row.join(" | ")}`);
      } else {
        const name = row[1];
        const category = row[2];
        const distance = row[row.length - 2];
        const similarity = row[row.length - 1];
        console.log(` #${i}  | Name: ${name} | Category: ${category} | Distance: ${distance} | Similarity: ${similarity}`);
      }
    });

    console.log("\n🎉 Sample 3 Vector Similarity Search completed successfully!");
  } catch (err) {
    console.error(`🚨 Sample 3 encountered an error: ${err.message}\n${err.stack}`);
    throw err;
  } finally {
    if (SAMPLE_CONFIG.AUTO_CLEANUP !== false) {
      cleanupAllSampleResources_({
        tables: [{ catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName }],
      });
    }
  }
}

// ============================================================================
// SAMPLE 4: Large Binary Multipart Upload (up to 50MB) via Drive API & Iceberg
// ============================================================================
/**
 * Sample 4 (Private):
 * Demonstrates high-performance binary asset ingestion (up to 50MB) using
 * multipart/form-data via Drive API v3 (Kanshi Tanaike architecture) and Apache Iceberg.
 *
 * @private
 */
function sample4_multipartUploadToDriveAndIceberg_() {
  console.log("=== [Sample 4] Multipart Upload (up to 50MB) to Drive & Iceberg ===");
  syncSampleConfigFromProperties_();

  if (!SAMPLE_CONFIG.PROJECT_ID || SAMPLE_CONFIG.PROJECT_ID === "your-gcp-project-id") {
    throw new Error("Please configure PROJECT_ID in ScriptProperties or set SAMPLE_CONFIG.PROJECT_ID with your GCP Project ID.");
  }

  // Pre-flight sweep: clean up any stale artifacts left from previous aborted runs
  cleanupAllSampleResources_();

  const tableName = "multipart_asset_catalog";
  let uploadedFileId = null;

  try {
    console.log(`Opening Iceberg Catalog [${SAMPLE_CONFIG.CATALOG_NAME}] and preparing asset table...`);
    const app = IcebergApp.openByCatalog(
      SAMPLE_CONFIG.PROJECT_ID,
      SAMPLE_CONFIG.CATALOG_NAME,
      SAMPLE_CONFIG.REGION
    ).ensureCatalog();

    const table = app.createAssetTable(tableName, {
      storageUri: SAMPLE_CONFIG.GCS_STORAGE_URI ? `${SAMPLE_CONFIG.GCS_STORAGE_URI}/${tableName}` : null,
      connection: SAMPLE_CONFIG.BIGLAKE_CONNECTION || null,
    });
    registerSampleResource_("tables", { catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName });
    console.log(`✅ Multipart asset table ready: ${table.getFullPath()}`);

    // 1. Prepare a sample binary asset payload for multipart upload
    console.log("1. Preparing binary asset payload for multipart upload...");
    const samplePayloadText = "=== IcebergApp High-Capacity Enterprise Lakehouse Asset ===\n" +
      "This binary payload demonstrates Kanshi Tanaike's multipart/form-data upload method,\n" +
      "enabling direct uploads up to 50MB (UrlFetchApp maximum payload limit) without hitting BigQuery's 1MB query size limit.\n" +
      "Timestamp: " + new Date().toISOString() + "\n" +
      "Author: Kanshi Tanaike\n" +
      "Payload Data: " + "X".repeat(50000); // 50KB+ synthetic payload

    const sampleBlob = Utilities.newBlob(samplePayloadText, "text/plain", `Iceberg_Sample_Multipart_${Date.now()}.txt`);

    // 2. Upload asset using table.uploadAsset (multipart/form-data via Drive API v3)
    console.log("2. Uploading binary asset via multipart/form-data to Drive & registering in Iceberg...");
    const uploadResult = table.uploadAsset(sampleBlob, {
      category: "multipart_large_asset",
      source: "Kanshi Tanaike Multipart Architecture",
      upload_type: "multipart/form-data",
    }, { destination: "drive" });

    uploadedFileId = uploadResult.fileId;
    if (uploadedFileId) {
      registerSampleResource_("docs", uploadedFileId);
    }
    console.log(`✅ Multipart Upload successful: File ID: [${uploadedFileId}], Size: ${uploadResult.size} bytes`);
    console.log(`   🔗 Web View URL: ${uploadResult.uri}`);

    // 3. Query Iceberg table to verify ingestion
    console.log("3. Querying Apache Iceberg table for the multipart asset...");
    const records = table.getValues({
      columns: ["id", "file_id", "name", "mime_type", "size", "url", "updated_at"],
      where: `file_id = '${uploadedFileId}'`,
    });

    console.log("--- Query Results from Apache Iceberg ---");
    records.forEach((row, i) => {
      console.log(`Row ${i}: ${row.join(" | ")}`);
    });

    console.log("🎉 Sample 4 Multipart Ingestion completed successfully!");
  } catch (err) {
    console.error(`🚨 Sample 4 encountered an error: ${err.message}\n${err.stack}`);
    throw err;
  } finally {
    if (SAMPLE_CONFIG.AUTO_CLEANUP !== false) {
      cleanupAllSampleResources_({
        docs: uploadedFileId ? [uploadedFileId] : [],
        tables: [{ catalog: SAMPLE_CONFIG.CATALOG_NAME, table: tableName }],
      });
    }
  }
}

// ============================================================================
// ALL-IN-ONE RUNNER
// ============================================================================
/**
 * Executes all 4 sample workflows sequentially with automated cleanups.
 */
function runAllSamples() {
  console.log("🚀 Starting Sequential Execution of All IcebergApp Samples...");
  
  // Prioritize PropertiesService for GEMINI_API_KEY, PROJECT_ID, REGION (default: 'asia-northeast1')
  syncSampleConfigFromProperties_();

  if (!SAMPLE_CONFIG.PROJECT_ID || SAMPLE_CONFIG.PROJECT_ID === "your-gcp-project-id") {
    throw new Error(
      "GCP Project ID is not configured. Please set 'PROJECT_ID' in ScriptProperties or configure SAMPLE_CONFIG.PROJECT_ID before running samples."
    );
  }

  console.log(`📋 Active Configuration: Project=[${SAMPLE_CONFIG.PROJECT_ID}], Catalog=[${SAMPLE_CONFIG.CATALOG_NAME}], Region=[${SAMPLE_CONFIG.REGION}]`);
  const apiKey = (typeof PropertiesService !== "undefined" && PropertiesService.getScriptProperties)
    ? PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY")
    : null;
  if (apiKey && apiKey.trim() !== "") {
    console.log(`🔑 Gemini API Key: Detected in PropertiesService. Vector search sample enabled.`);
  } else {
    console.log(`ℹ️ Gemini API Key: Not configured in PropertiesService. (Sample 3 vector search will be skipped).`);
  }

  cleanupAllSampleResources_(); // Pre-flight sweep

  try {
    console.log("\n>>> [1/4] Running Sample 1 (Google Docs Ingestion)...");
    sample1_importGoogleDocToIceberg_();

    console.log("\n>>> [2/4] Running Sample 2 (External Image Blob Ingestion)...");
    sample2_importExternalImageBlobToIceberg_();

    if (apiKey && apiKey.trim() !== "") {
      console.log("\n>>> [3/4] Running Sample 3 (Gemini Vector Search)...");
      sample3_vectorSearchWithGeminiEmbedding_();
    } else {
      console.log("\nℹ️ [3/4] Sample 3 skipped because GEMINI_API_KEY is not configured in ScriptProperties.");
    }

    console.log("\n>>> [4/4] Running Sample 4 (Multipart Form-Data Upload up to 50MB)...");
    sample4_multipartUploadToDriveAndIceberg_();
  } finally {
    cleanupAllSampleResources_();
    console.log("\n🎉 All samples finished and all resources 100% cleaned up.");
  }
}

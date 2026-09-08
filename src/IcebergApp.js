/**
 * GitHub: https://github.com/tanaikech/IcebergApp
 * IcebergApp: A Google Apps Script library for managing Google Cloud Lakehouse for Apache Iceberg.
 * Leverages BigQuery Advanced Service (v2) and Iceberg REST Catalog / Lakehouse architecture.
 *
 * Copyright (c) 2026 Kanshi Tanaike
 * Licensed under the MIT License
 */

/**
 * Entry point: Opens the IcebergApp instance for a specific Lakehouse Catalog (BigQuery Dataset).
 *
 * @param {string} projectId Google Cloud Project ID.
 * @param {string} catalogName The Lakehouse Catalog or BigQuery Dataset name.
 * @param {string} [location="asia-northeast1"] Google Cloud Region.
 * @return {IcebergApp} An instance of IcebergApp.
 */
function openByCatalog(projectId, catalogName, location = "asia-northeast1") {
  return new IcebergApp(projectId, catalogName, location);
}

/**
 * Class for managing Apache Iceberg tables and metadata operations.
 */
class IcebergApp {
  /**
   * Static factory entry point.
   *
   * @param {string} projectId Google Cloud Project ID.
   * @param {string} catalogName The Lakehouse Catalog or BigQuery Dataset name.
   * @param {string} [location="asia-northeast1"] Google Cloud Region.
   * @return {IcebergApp} An instance of IcebergApp.
   */
  static openByCatalog(projectId, catalogName, location = "asia-northeast1") {
    return new IcebergApp(projectId, catalogName, location);
  }

  /**
   * Generates a vector embedding using the Gemini API if GEMINI_API_KEY is configured.
   * Supports both text strings and Google Apps Script binary Blobs (images, documents, audio).
   *
   * @param {string|GoogleAppsScript.Base.Blob} input Text or binary Blob.
   * @param {string} [customApiKey=null] Optional API key override (defaults to Script/UserProperties GEMINI_API_KEY).
   * @param {Object} [options={}] Optional configuration (e.g. { model: 'text-embedding-004' }).
   * @return {Array<number>} Vector of floats.
   */
  static generateEmbedding(input, customApiKey = null, options = {}) {
    const apiKey = (customApiKey && typeof customApiKey === "string" && customApiKey.trim() !== "")
      ? customApiKey.trim()
      : getGeminiApiKey_();
    if (!apiKey) {
      throw new Error(
        "GEMINI_API_KEY is not configured in ScriptProperties or UserProperties. Please configure GEMINI_API_KEY to generate embeddings."
      );
    }
    return getGeminiEmbedding_(input, apiKey, options);
  }

  /**
   * Uploads a binary Blob up to 50MB directly to Google Drive using Drive API v3 multipart/form-data.
   * Based on Kanshi Tanaike's multipart upload architecture for Google Apps Script.
   *
   * @param {GoogleAppsScript.Base.Blob} blob Binary file Blob.
   * @param {Object} [metadata={}] Drive metadata (name, mimeType, parents, description).
   * @return {{id: string, name: string, mimeType: string, size: number, webViewLink: string, url: string}}
   */
  static uploadToDrive(blob, metadata = {}) {
    return uploadToDriveMultipart_(blob, metadata);
  }

  /**
   * Uploads a binary Blob up to 50MB directly to Google Cloud Storage (GCS).
   *
   * @param {GoogleAppsScript.Base.Blob} blob Binary file Blob.
   * @param {string} bucketName Target GCS Bucket name.
   * @param {string} [objectPath] Target object path.
   * @param {Object} [customMetadata={}] Optional user metadata.
   * @return {{bucket: string, name: string, uri: string, size: number, md5Hash: string, contentType: string}}
   */
  static uploadToStorage(blob, bucketName, objectPath = "", customMetadata = {}) {
    return uploadToGcsMultipart_(blob, bucketName, objectPath, customMetadata);
  }

  /**
   * @param {string} projectId Google Cloud Project ID.
   * @param {string} catalogName Lakehouse catalog / dataset name.
   * @param {string} [location="asia-northeast1"] Google Cloud Region.
   */
  constructor(projectId, catalogName, location = "asia-northeast1") {
    if (!projectId || typeof projectId !== "string" || projectId.trim() === "") {
      throw new Error("Parameter 'projectId' must be a non-empty string.");
    }
    if (!catalogName || typeof catalogName !== "string" || catalogName.trim() === "") {
      throw new Error("Parameter 'catalogName' must be a non-empty string.");
    }
    /** @private @type {string} */
    this.projectId = projectId.trim();
    /** @private @type {string} */
    this.catalogName = catalogName.trim();
    /** @private @type {string} */
    this.location = (location && typeof location === "string" && location.trim() !== "")
      ? location.trim()
      : "asia-northeast1";
  }

  /**
   * Ensures that the BigQuery Dataset (Lakehouse Catalog) exists in the designated location.
   * If the dataset does not exist, it is created automatically.
   *
   * @param {Object} [options={}] Configuration options.
   * @param {string} [options.description] Optional description for the dataset.
   * @return {IcebergApp} This instance for method chaining.
   */
  ensureCatalog(options = {}) {
    ensureBigQueryDataset_(this.projectId, this.catalogName, this.location, options.description);
    return this;
  }

  /**
   * Creates a new Apache Iceberg / BigLake table with specified schema.
   * Automatically ensures the parent catalog/dataset exists prior to creation.
   * When `options.connection` is specified, generates an Apache Iceberg managed table DDL with WITH CONNECTION.
   * When `options.connection` is omitted, provisions a Lakehouse-optimized BigQuery table.
   *
   * @param {string} tableName Target table name.
   * @param {Object} options Configuration parameters.
   * @param {Array<{name: string, type: string, mode?: string}>} options.schema Field definitions.
   * @param {string} [options.storageUri] Target Cloud Storage location (e.g., 'gs://bucket/path').
   * @param {string} [options.connection] BigLake Cloud Resource connection (e.g., 'projects/.../connections/...' or 'connection_name').
   * @param {Array<string>} [options.partitionBy] Column or expression for partitioning (e.g. ['DATE(created_at)']).
   * @param {Array<string>} [options.clusterBy] Column names for clustering (e.g. ['id']).
   * @param {boolean} [options.autoCreateCatalog=true] Whether to auto-create BigQuery dataset if absent.
   * @return {IcebergTable} Created IcebergTable instance.
   */
  create(tableName, options) {
    if (!tableName || typeof tableName !== "string" || tableName.trim() === "") {
      throw new Error("Parameter 'tableName' must be a non-empty string.");
    }
    if (!options || typeof options !== "object") {
      throw new Error("Parameter 'options' must be a valid configuration object.");
    }
    if (!Array.isArray(options.schema) || options.schema.length === 0) {
      throw new Error("Parameter 'options.schema' must be a non-empty array of column definitions.");
    }

    // Auto-ensure catalog dataset exists prior to DDL execution
    if (options.autoCreateCatalog !== false) {
      ensureBigQueryDataset_(this.projectId, this.catalogName, this.location);
    }

    const schemaDefinition = options.schema
      .map((col, idx) => {
        if (!col || typeof col.name !== "string" || typeof col.type !== "string") {
          throw new Error(`Invalid schema definition at index ${idx}: 'name' and 'type' are required strings.`);
        }
        const colName = col.name.trim().replace(/`/g, "");
        const colType = col.type.trim().toUpperCase();
        const mode = col.mode === "REQUIRED" ? " NOT NULL" : "";
        return `\`${colName}\` ${colType}${mode}`;
      })
      .join(",\n  ");

    // Partitioning & Clustering clauses
    let partitionClause = "";
    if (Array.isArray(options.partitionBy) && options.partitionBy.length > 0) {
      // BigQuery Standard SQL requires expressions (like DATE(col)) or supported types.
      // Filter out raw integer columns that would trigger BigQuery syntax errors without RANGE_BUCKET.
      const validPartitions = options.partitionBy.filter((p) => {
        const clean = String(p).trim();
        const matchingCol = options.schema.find((s) => s.name === clean);
        if (matchingCol && (matchingCol.type.toUpperCase() === "INT64" || matchingCol.type.toUpperCase() === "INTEGER") && !clean.includes("(")) {
          return false;
        }
        return true;
      });

      if (validPartitions.length > 0) {
        const parts = validPartitions.map((p) => {
          const clean = String(p).trim();
          return clean.includes("(") ? clean : `\`${clean.replace(/`/g, "")}\``;
        });
        partitionClause = `\nPARTITION BY ${parts.join(", ")}`;
      }
    }

    let clusterClause = "";
    let clusterCols = Array.isArray(options.clusterBy) ? [...options.clusterBy] : [];
    if (clusterCols.length === 0 && Array.isArray(options.partitionBy)) {
      options.partitionBy.forEach((p) => {
        const clean = String(p).trim();
        const matchingCol = options.schema.find((s) => s.name === clean);
        if (matchingCol && (matchingCol.type.toUpperCase() === "INT64" || matchingCol.type.toUpperCase() === "INTEGER") && !clean.includes("(")) {
          clusterCols.push(clean);
        }
      });
    }

    // Defensively filter clusterCols to ONLY columns that actually exist in options.schema
    if (Array.isArray(options.schema) && options.schema.length > 0) {
      const schemaColNames = new Set(options.schema.map((s) => s.name));
      clusterCols = clusterCols.filter((c) => schemaColNames.has(String(c).trim().replace(/`/g, "")));
    }

    if (clusterCols.length > 0) {
      const uniqueClusterCols = [...new Set(clusterCols)].slice(0, 4);
      clusterClause = `\nCLUSTER BY ${uniqueClusterCols.map((c) => `\`${String(c).trim().replace(/`/g, "")}\``).join(", ")}`;
    }

    // Connection and Iceberg Table Options
    // BigQuery requires WITH CONNECTION clause when table_format = 'ICEBERG' is present.
    let connectionClause = "";
    let optionsClause = "";

    const cleanStorageUri = (options.storageUri && typeof options.storageUri === "string" && options.storageUri.trim() !== "")
      ? options.storageUri.trim()
      : null;

    if (options.connection && typeof options.connection === "string" && options.connection.trim() !== "") {
      const conn = options.connection.trim();
      const formattedConn = conn.toUpperCase() === "DEFAULT"
        ? "DEFAULT"
        : (conn.includes("/") ? `\`${conn.replace(/`/g, "")}\`` : `\`${this.projectId}.${this.location}.${conn.replace(/`/g, "")}\``);
      
      connectionClause = `\nWITH CONNECTION ${formattedConn}`;

      const tableOptions = [`table_format = 'ICEBERG'`];
      if (cleanStorageUri) {
        const cleanUri = cleanStorageUri.replace(/'/g, "\\'");
        tableOptions.push(`storage_uri = '${cleanUri}'`);
      }
      optionsClause = `\nOPTIONS (\n  ${tableOptions.join(",\n  ")}\n)`;
    }

    const cleanTableName = tableName.trim().replace(/`/g, "");
    const ddl = `
      CREATE OR REPLACE TABLE \`${this.projectId}.${this.catalogName}.${cleanTableName}\` (
        ${schemaDefinition}
      )${partitionClause}${clusterClause}${connectionClause}${optionsClause};
    `;

    this._runQuery(ddl);

    return new IcebergTable({
      projectId: this.projectId,
      catalogName: this.catalogName,
      tableName: cleanTableName,
      location: this.location,
      storageUri: cleanStorageUri,
    });
  }

  /**
   * Creates an asset table pre-configured for binary Blobs and Google Drive files with rich metadata
   * (id, file_id, name, mime_type, size, data, embedding, updated_at).
   *
   * @param {string} tableName Name of the asset table.
   * @param {Object} [options={}] Additional table options.
   * @param {Array<{name: string, type: string, mode?: string}>} [options.schema] Custom schema overriding default.
   * @param {string} [options.storageUri] Target Cloud Storage location (e.g. 'gs://bucket/path').
   * @param {string} [options.connection] BigLake Cloud Resource connection.
   * @param {Array<string>} [options.clusterBy] Clustering columns (defaults to ['file_id', 'mime_type']).
   * @return {IcebergTable} Created IcebergTable instance.
   */
  createAssetTable(tableName, options = {}) {
    const defaultSchema = [
      { name: "id", type: "STRING", mode: "REQUIRED" },
      { name: "file_id", type: "STRING" },
      { name: "name", type: "STRING" },
      { name: "mime_type", type: "STRING" },
      { name: "source_mime_type", type: "STRING" },
      { name: "url", type: "STRING" },
      { name: "size", type: "INT64" },
      { name: "metadata", type: "STRING" },
      { name: "data", type: "BYTES" },
      { name: "embedding", type: "ARRAY<FLOAT64>" },
      { name: "updated_at", type: "TIMESTAMP" },
    ];

    const mergedOptions = Object.assign({}, options, {
      schema: Array.isArray(options.schema) ? options.schema : defaultSchema,
      clusterBy: options.clusterBy || ["file_id", "mime_type"],
    });

    return this.create(tableName, mergedOptions);
  }

  /**
   * Lists all tables in the current catalog.
   *
   * @return {Array<IcebergTable>} Array of IcebergTable instances.
   */
  getTables() {
    const query = `
      SELECT table_name 
      FROM \`${this.projectId}.${this.catalogName}.INFORMATION_SCHEMA.TABLES\`
      WHERE table_type IN ('EXTERNAL', 'BASE TABLE');
    `;
    const result = this._runQuery(query);
    return result.rows.map(
      (r) =>
        new IcebergTable({
          projectId: this.projectId,
          catalogName: this.catalogName,
          tableName: r.table_name,
          location: this.location,
        }),
    );
  }

  /**
   * Retrieves an IcebergTable instance by table name.
   *
   * @param {string} tableName Name of the table.
   * @return {IcebergTable|null} The instance or null if not found.
   */
  getTableByName(tableName) {
    if (!tableName || typeof tableName !== "string") return null;
    const cleanName = tableName.trim();
    const tables = this.getTables();
    return tables.find((t) => t.getName() === cleanName) || null;
  }

  /**
   * Directly returns an IcebergTable instance for the specified table name
   * without querying INFORMATION_SCHEMA.
   *
   * @param {string} tableName Name of the table.
   * @return {IcebergTable} The IcebergTable instance.
   */
  getTable(tableName) {
    if (!tableName || typeof tableName !== "string" || tableName.trim() === "") {
      throw new Error("Parameter 'tableName' must be a non-empty string.");
    }
    return new IcebergTable({
      projectId: this.projectId,
      catalogName: this.catalogName,
      tableName: tableName.trim(),
      location: this.location,
    });
  }

  /**
   * Internal query runner.
   *
   * @private
   * @param {string} query SQL statement.
   * @return {{rows: Array<Object>, affectedRows: number}} Result set and execution metadata.
   */
  _runQuery(query) {
    try {
      return runBqJob_(this.projectId, query, this.location);
    } catch (err) {
      const errMsg = err.message || String(err);
      if (errMsg.indexOf("Not found: Dataset") !== -1 || errMsg.indexOf("was not found") !== -1) {
        console.log(`⚡ Catalog Dataset [${this.catalogName}] absent. Auto-provisioning at [${this.location}]...`);
        ensureBigQueryDataset_(this.projectId, this.catalogName, this.location);
        return runBqJob_(this.projectId, query, this.location);
      }
      throw err;
    }
  }
}

/**
 * Class representing an individual Apache Iceberg Table instance.
 */
class IcebergTable {
  /**
   * @param {Object} obj Configuration object.
   * @param {string} obj.projectId
   * @param {string} obj.catalogName
   * @param {string} obj.tableName
   * @param {string} [obj.location]
   */
  constructor(obj) {
    if (!obj || typeof obj !== "object") {
      throw new Error("IcebergTable requires a configuration object.");
    }
    if (!obj.projectId || !obj.catalogName || !obj.tableName) {
      throw new Error("IcebergTable requires 'projectId', 'catalogName', and 'tableName'.");
    }
    /** @private */
    this.projectId = String(obj.projectId).trim();
    /** @private */
    this.catalogName = String(obj.catalogName).trim();
    /** @private */
    this.tableName = String(obj.tableName).trim();
    /** @private */
    this.location = obj.location || "asia-northeast1";
    /** @private @type {string|null} */
    this.storageUri = obj.storageUri ? String(obj.storageUri).trim() : null;
    /** @private @type {Date|null} */
    this.timeTravelSnapshot = null;
  }

  /**
   * Gets the base Cloud Storage URI configured for this table.
   * @return {string|null} Storage URI.
   */
  getStorageUri() {
    return this.storageUri;
  }

  /**
   * Sets or updates the base Cloud Storage URI for this table.
   * @param {string} uri Cloud Storage URI (e.g. 'gs://bucket/path').
   * @return {IcebergTable} This instance for method chaining.
   */
  setStorageUri(uri) {
    this.storageUri = uri ? String(uri).trim() : null;
    return this;
  }

  /**
   * Gets the name of the table.
   * @return {string} Table name.
   */
  getName() {
    return this.tableName;
  }

  /**
   * Gets the fully qualified table path for SQL statements.
   * @return {string} Escaped full path.
   */
  getFullPath() {
    const p = this.projectId.replace(/`/g, "");
    const c = this.catalogName.replace(/`/g, "");
    const t = this.tableName.replace(/`/g, "");
    return `\`${p}.${c}.${t}\``;
  }

  /**
   * Retrieves the column names for this table from BigQuery or cached metadata.
   *
   * @return {Array<string>|null} Array of column names, or null if unresolvable.
   */
  getColumnNames() {
    if (this._cachedColumns && Array.isArray(this._cachedColumns) && this._cachedColumns.length > 0) {
      return this._cachedColumns;
    }

    try {
      if (typeof BigQuery !== "undefined" && BigQuery.Tables && typeof BigQuery.Tables.get === "function") {
        const tb = BigQuery.Tables.get(this.projectId, this.catalogName, this.tableName);
        if (tb && tb.schema && Array.isArray(tb.schema.fields)) {
          this._cachedColumns = tb.schema.fields.map((f) => f.name);
          return this._cachedColumns;
        }
      }
    } catch (e) {}

    try {
      const q = `SELECT column_name FROM \`${this.projectId}.${this.catalogName}.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = '${this.tableName}';`;
      const res = runBqJobRawValues_(this.projectId, q, this.location);
      if (Array.isArray(res) && res.length > 1) {
        this._cachedColumns = res.slice(1).map((r) => r[0]);
        return this._cachedColumns;
      }
    } catch (e) {}

    return null;
  }

  /**
   * Applies Time Travel snapshot constraint for subsequent queries.
   *
   * @param {Date|string|number} timestamp Snapshot target date, ISO string, or epoch milliseconds.
   * @return {IcebergTable} This instance for method chaining.
   */
  asOf(timestamp) {
    if (!timestamp) {
      throw new Error("Parameter 'timestamp' must be a valid Date, ISO string, or epoch milliseconds.");
    }
    const d = timestamp instanceof Date ? timestamp : new Date(timestamp);
    if (isNaN(d.getTime())) {
      throw new Error(`Invalid Date value provided to asOf(): ${timestamp}`);
    }
    this.timeTravelSnapshot = d;
    return this;
  }

  /**
   * Resets Time Travel constraint.
   * @return {IcebergTable} This instance for method chaining.
   */
  resetSnapshot() {
    this.timeTravelSnapshot = null;
    return this;
  }

  /**
   * Queries and returns records as a 2D array (first row contains headers).
   * Supports Predicate Pushdown and column projection.
   *
   * @param {Object} [filter={}] Query filters.
   * @param {string} [filter.where] WHERE clause condition without the 'WHERE' keyword.
   * @param {Array<string>} [filter.columns] Specific column names to fetch.
   * @param {string} [filter.where] Predicate condition.
   * @param {string} [filter.orderBy] Sort expression, e.g. "id ASC" or "updated_at DESC".
   * @param {number} [filter.limit] Maximum row count to return.
   * @return {Array<Array<any>>} 2D array formatted for Sheet values.
   */
  getValues(filter = {}) {
    const opts = (filter && typeof filter === "object") ? filter : {};

    let columns = "*";
    if (Array.isArray(opts.columns) && opts.columns.length > 0) {
      columns = opts.columns.map((c) => `\`${String(c).trim().replace(/`/g, "")}\``).join(", ");
    }

    let timeTravelClause = "";
    if (this.timeTravelSnapshot) {
      timeTravelClause = `FOR SYSTEM_TIME AS OF TIMESTAMP('${this.timeTravelSnapshot.toISOString()}')`;
    }

    let whereClause = "";
    if (opts.where && typeof opts.where === "string" && opts.where.trim() !== "") {
      if (/;\s*$/m.test(opts.where) || /;\s*[a-zA-Z]/m.test(opts.where)) {
        throw new Error("Semicolons are prohibited in where clause to prevent stacked queries.");
      }
      const strippedWhere = opts.where.trim().replace(/^WHERE\s+/i, "");
      whereClause = `WHERE ${strippedWhere}`;
    }

    let orderByClause = "";
    if (opts.orderBy && typeof opts.orderBy === "string" && opts.orderBy.trim() !== "") {
      if (/;\s*$/m.test(opts.orderBy) || /;\s*[a-zA-Z]/m.test(opts.orderBy)) {
        throw new Error("Semicolons are prohibited in orderBy clause.");
      }
      const strippedOrder = opts.orderBy.trim().replace(/^ORDER\s+BY\s+/i, "");
      orderByClause = `ORDER BY ${strippedOrder}`;
    }

    let limitClause = "";
    if (opts.limit !== undefined && opts.limit !== null) {
      const limitNum = Number(opts.limit);
      if (!Number.isInteger(limitNum) || limitNum < 0) {
        throw new Error("Parameter 'filter.limit' must be a non-negative integer.");
      }
      limitClause = `LIMIT ${limitNum}`;
    }

    const sql = `
      SELECT ${columns} 
      FROM ${this.getFullPath()} ${timeTravelClause}
      ${whereClause}
      ${orderByClause}
      ${limitClause};
    `;

    return runBqJobRawValues_(this.projectId, sql, this.location);
  }

  /**
   * Executes a vector similarity search against the Iceberg table using Gemini Embeddings and BigQuery COSINE_DISTANCE.
   * Enables semantic searching across text documents, Drive files, and multimodal assets.
   *
   * @param {string|GoogleAppsScript.Base.Blob|Array<number>} query Search query (natural language string, Blob, or raw vector).
   * @param {Object} [options={}] Configuration options.
   * @param {number} [options.topK=5] Maximum number of most similar records to return.
   * @param {string} [options.embeddingColumn="embedding"] Column containing vector embeddings.
   * @param {Array<string>} [options.columns] Projected columns to return (defaults to standard asset metadata).
   * @param {string} [options.where] Additional SQL filter predicate (e.g. "mime_type = 'application/pdf'").
   * @param {string} [options.apiKey] Optional Gemini API key override.
   * @param {string} [options.distanceMetric="COSINE"] 'COSINE' (COSINE_DISTANCE) or 'EUCLIDEAN' (EUCLIDEAN_DISTANCE).
   * @param {Object} [options.embeddingOptions={}] Options forwarded to Gemini Embedding API.
   * @return {Array<Array<any>>} 2D array including header row, with calculated distance and similarity scores.
   */
  searchSimilar(query, options = {}) {
    if (query === null || query === undefined) {
      throw new Error("Query parameter is required for vector similarity search.");
    }

    let queryVector;
    if (Array.isArray(query) && query.length > 0 && typeof query[0] === "number") {
      queryVector = query;
    } else {
      const apiKey = (options.apiKey && typeof options.apiKey === "string" && options.apiKey.trim() !== "")
        ? options.apiKey.trim()
        : getGeminiApiKey_();
      if (!apiKey) {
        throw new Error(
          "GEMINI_API_KEY is not configured in ScriptProperties or UserProperties. Please configure GEMINI_API_KEY to execute vector search with text/blob queries."
        );
      }
      queryVector = getGeminiEmbedding_(query, apiKey, options.embeddingOptions || {});
    }

    const topK = Math.max(1, Number(options.topK) || 5);
    const embCol = (options.embeddingColumn || "embedding").replace(/`/g, "");
    const metric = (options.distanceMetric || "COSINE").toUpperCase() === "EUCLIDEAN"
      ? "EUCLIDEAN_DISTANCE"
      : "COSINE_DISTANCE";

    const defaultCols = ["id", "file_id", "name", "mime_type", "size", "updated_at"];
    const selectCols = (Array.isArray(options.columns) && options.columns.length > 0)
      ? options.columns.map((c) => `\`${String(c).trim().replace(/`/g, "")}\``).join(", ")
      : defaultCols.map((c) => `\`${c}\``).join(", ");

    const extraWhere = options.where && typeof options.where === "string" && options.where.trim() !== ""
      ? `AND (${options.where.trim()})`
      : "";

    const vectorLiteral = `[${queryVector.join(", ")}]`;

    const sql = `
      SELECT
        ${selectCols},
        ${metric}(\`${embCol}\`, ${vectorLiteral}) AS distance,
        ROUND(1.0 - ${metric}(\`${embCol}\`, ${vectorLiteral}), 4) AS similarity
      FROM ${this.getFullPath()}
      WHERE \`${embCol}\` IS NOT NULL ${extraWhere}
      ORDER BY distance ASC
      LIMIT ${topK};
    `;

    return runBqJobRawValues_(this.projectId, sql, this.location);
  }

  /**
   * Appends records into the table using a 2D array (first row must be column names).
   *
   * @param {Array<Array<any>>} values 2D array including headers.
   * @return {number} Count of successfully inserted rows.
   */
  insertValues(values) {
    if (!Array.isArray(values) || values.length < 2) {
      throw new Error("Invalid format. Values must contain a header row and at least one data row.");
    }

    const headers = values[0];
    if (!Array.isArray(headers) || headers.length === 0) {
      throw new Error("Invalid header row. Must be a non-empty array of column names.");
    }
    const expectedColCount = headers.length;
    const dataRows = values.slice(1);

    const formattedRows = dataRows.map((row, rIdx) => {
      if (!Array.isArray(row)) {
        throw new Error(`Invalid data row at index ${rIdx + 1}: Expected an array.`);
      }
      if (row.length !== expectedColCount) {
        throw new Error(
          `Dimension mismatch at row ${rIdx + 1}: expected ${expectedColCount} columns, received ${row.length}.`
        );
      }
      const escaped = row.map((val) => formatSqlValue_(val, rIdx + 1)).join(", ");
      return `(${escaped})`;
    }).join(",\n");

    const sql = `
      INSERT INTO ${this.getFullPath()} (${headers.map((h) => `\`${String(h).trim().replace(/`/g, "")}\``).join(", ")})
      VALUES ${formattedRows};
    `;

    const res = runBqJob_(this.projectId, sql, this.location);
    return res.affectedRows > 0 ? res.affectedRows : dataRows.length;
  }

  /**
   * Generates a vector embedding for text or binary Blob using the configured GEMINI_API_KEY.
   *
   * @param {string|GoogleAppsScript.Base.Blob} input Text string or binary Blob.
   * @param {string} [customApiKey=null] Optional API key override.
   * @param {Object} [options={}] Optional configuration parameters.
   * @return {Array<number>} Vector of floats.
   */
  generateEmbedding(input, customApiKey = null, options = {}) {
    return IcebergApp.generateEmbedding(input, customApiKey, options);
  }

  /**
   * Appends records into the table with automated Gemini vector embedding generation.
   * If a record contains text or binary Blob in `options.embeddingSourceColumn`,
   * generates vector embeddings via Gemini API and populates `options.embeddingTargetColumn`.
   *
   * @param {Array<Array<any>>} values 2D array including headers.
   * @param {Object} options Configuration options.
   * @param {string} options.embeddingSourceColumn Header name containing text or Blob to embed.
   * @param {string} [options.embeddingTargetColumn="embedding"] Target column name for the float vector.
   * @param {string} [options.apiKey] Optional Gemini API key override (defaults to Script/UserProperties GEMINI_API_KEY).
   * @param {Object} [options.embeddingOptions] Optional parameters passed to Gemini API.
   * @return {number} Count of successfully inserted rows.
   */
  insertValuesWithEmbedding(values, options = {}) {
    if (!Array.isArray(values) || values.length < 2) {
      throw new Error("Invalid format. Values must contain a header row and at least one data row.");
    }
    if (!options || typeof options !== "object") {
      throw new Error("Options object with 'embeddingSourceColumn' is required.");
    }
    const sourceCol = options.embeddingSourceColumn;
    if (!sourceCol || typeof sourceCol !== "string" || sourceCol.trim() === "") {
      throw new Error("Parameter 'options.embeddingSourceColumn' must be a non-empty string.");
    }

    const targetCol = (options.embeddingTargetColumn && typeof options.embeddingTargetColumn === "string")
      ? options.embeddingTargetColumn.trim()
      : "embedding";

    const apiKey = (options.apiKey && typeof options.apiKey === "string" && options.apiKey.trim() !== "")
      ? options.apiKey.trim()
      : getGeminiApiKey_();

    if (!apiKey) {
      throw new Error(
        "GEMINI_API_KEY is not configured in ScriptProperties or UserProperties. Please configure GEMINI_API_KEY to generate embeddings."
      );
    }

    const headers = [...values[0]];
    const sourceIdx = headers.indexOf(sourceCol.trim());
    if (sourceIdx === -1) {
      throw new Error(`Embedding source column '${sourceCol}' was not found in headers: [${headers.join(", ")}].`);
    }

    let targetIdx = headers.indexOf(targetCol);
    if (targetIdx === -1) {
      headers.push(targetCol);
      targetIdx = headers.length - 1;
    }

    const enrichedValues = [headers];

    for (let r = 1; r < values.length; r++) {
      const row = [...values[r]];
      while (row.length < headers.length) {
        row.push(null);
      }
      const sourceVal = row[sourceIdx];
      if (sourceVal !== null && sourceVal !== undefined && sourceVal !== "") {
        const embedding = getGeminiEmbedding_(sourceVal, apiKey, options.embeddingOptions || {});
        row[targetIdx] = embedding;
      }
      enrichedValues.push(row);
    }

    return this.insertValues(enrichedValues);
  }

  /**
   * Inserts a Google Apps Script binary Blob directly into the Iceberg table.
   * Automatically extracts file metadata (name, MIME type, size) and converts binary data to BigQuery BYTES.
   * If GEMINI_API_KEY is present (or options.embed is true), computes and stores multimodal vector embeddings.
   *
   * @param {GoogleAppsScript.Base.Blob} blob The binary Blob object (image, PDF, audio, doc, etc.).
   * @param {Object} [metadata={}] Optional user-defined metadata columns (e.g. { id: 101, file_id: "...", category: "spec" }).
   * @param {Object} [options={}] Configuration options.
   * @param {boolean} [options.embed] Whether to compute Gemini embedding (defaults to true if GEMINI_API_KEY is configured).
   * @param {string} [options.embeddingColumn="embedding"] Column name for the embedding vector.
   * @param {Object} [options.columnMap] Column mapping object.
   * @return {number} Count of affected rows (1 on success).
   */
  insertBlob(blob, metadata = {}, options = {}) {
    if (!isBlob_(blob)) {
      throw new Error("Invalid argument: 'blob' must be a valid Google Apps Script Blob object.");
    }
    const byteSize = blob.getBytes().length;
    const targetStorageUri = options.storageUri || this.storageUri || null;

    if ((options.uploadToStorage || (byteSize > 700 * 1024 && targetStorageUri)) && targetStorageUri) {
      return this.uploadAsset(blob, metadata, Object.assign({}, options, { storageUri: targetStorageUri, destination: "storage" })).success ? 1 : 0;
    }

    return this.insertBlobs([{ blob: blob, metadata: metadata }], options);
  }

  /**
   * Uploads a binary asset (Blob up to 50MB) to Google Cloud Storage or Google Drive using
   * multipart/form-data, and registers its URI, metadata, and Gemini vector embedding
   * into the Apache Iceberg table.
   * Based on Kanshi Tanaike's multipart upload architecture for Google Apps Script.
   *
   * @param {GoogleAppsScript.Base.Blob} blob Binary file Blob (up to 50MB).
   * @param {Object} [metadata={}] Additional metadata attributes.
   * @param {Object} [options={}] Configuration options.
   * @param {string} [options.destination="storage"] Destination: "storage" (Cloud Storage) or "drive" (Google Drive).
   * @param {string} [options.storageUri] Target Cloud Storage base URI (e.g. 'gs://bucket/assets'). Defaults to table's storageUri.
   * @param {string} [options.bucket] Target GCS bucket name.
   * @param {boolean} [options.storeInlineBytes=false] Whether to also store bytes inline in BigQuery if size < 500KB.
   * @param {boolean} [options.embed=true] Whether to compute Gemini vector embeddings.
   * @return {{success: boolean, id: string, name: string, uri: string, fileId: string|null, mimeType: string, size: number}} Upload summary.
   */
  uploadAsset(blob, metadata = {}, options = {}) {
    if (!isBlob_(blob)) {
      throw new Error("uploadAsset requires a valid Google Apps Script Blob.");
    }

    const destination = options.destination || ((options.storageUri || this.storageUri || options.bucket) ? "storage" : "drive");
    let assetUri = null;
    let fileId = null;
    const finalMeta = Object.assign({}, metadata);

    if (destination === "storage") {
      const baseUri = options.storageUri || this.storageUri || "";
      let bucket = options.bucket;
      let pathPrefix = "";

      if (!bucket && baseUri) {
        const parts = baseUri.replace(/^gs:\/\//, "").split("/");
        bucket = parts[0];
        pathPrefix = parts.slice(1).join("/");
      }

      if (!bucket) {
        throw new Error("GCS Bucket or storageUri must be specified for Cloud Storage upload.");
      }

      const fileName = blob.getName() || `asset_${Date.now()}`;
      const objectPath = pathPrefix ? `${pathPrefix.replace(/\/$/, "")}/${fileName}` : fileName;

      const uploadRes = uploadToGcsMultipart_(blob, bucket, objectPath, metadata);
      assetUri = uploadRes.uri;
      finalMeta.url = assetUri;
      finalMeta.storage_uri = assetUri;
      if (!finalMeta.name) finalMeta.name = blob.getName();
    } else {
      const uploadRes = uploadToDriveMultipart_(blob, metadata);
      fileId = uploadRes.id;
      assetUri = uploadRes.webViewLink;
      finalMeta.file_id = fileId;
      finalMeta.url = assetUri;
      if (!finalMeta.name) finalMeta.name = blob.getName();
    }

    const byteSize = blob.getBytes().length;
    finalMeta.size = byteSize;
    const shouldStoreInline = Boolean(options.storeInlineBytes && byteSize < 500 * 1024);
    const blobToInsert = shouldStoreInline
      ? blob
      : Utilities.newBlob("", blob.getContentType() || "application/octet-stream", blob.getName());

    const combinedOptions = Object.assign({}, options, {
      destinationUri: assetUri,
    });

    this.insertBlob(blobToInsert, finalMeta, combinedOptions);

    return {
      success: true,
      id: finalMeta.id || null,
      name: blob.getName(),
      uri: assetUri,
      fileId: fileId,
      mimeType: blob.getContentType(),
      size: byteSize,
    };
  }

  /**
   * Batch inserts multiple Google Apps Script binary Blobs into the Iceberg table in a single atomic query.
   *
   * @param {Array<{blob: GoogleAppsScript.Base.Blob, metadata?: Object}|GoogleAppsScript.Base.Blob>} blobEntries Array of Blobs or Blob-metadata objects.
   * @param {Object} [options={}] Configuration options.
   * @param {boolean} [options.embed] Whether to compute Gemini embedding.
   * @param {string} [options.embeddingColumn="embedding"] Target column name for embedding vector.
   * @param {Object} [options.columnMap] Custom column name mapping.
   * @param {string} [options.apiKey] Optional Gemini API key override.
   * @return {number} Count of successfully inserted rows.
   */
  insertBlobs(blobEntries, options = {}) {
    if (!Array.isArray(blobEntries) || blobEntries.length === 0) {
      throw new Error("Invalid argument: 'blobEntries' must be a non-empty array of Blobs or {blob, metadata} entries.");
    }

    const colMap = Object.assign({
      id: "id",
      fileId: "file_id",
      name: "name",
      mimeType: "mime_type",
      size: "size",
      data: "data",
      embedding: "embedding",
      updatedAt: "updated_at",
    }, options.columnMap || {});

    const apiKey = (options.apiKey && typeof options.apiKey === "string" && options.apiKey.trim() !== "")
      ? options.apiKey.trim()
      : getGeminiApiKey_();

    const shouldEmbed = (options.embed !== false) && Boolean(apiKey);

    // Standard headers
    // Fetch available columns in the table to avoid SQL errors when extra metadata is passed
    const tableColumns = this.getColumnNames();
    const hasCol = (colName) => {
      if (!tableColumns || !Array.isArray(tableColumns)) return true;
      return tableColumns.includes(colName);
    };

    // Standard headers that actually exist in the table
    const headers = [];
    if (hasCol(colMap.id)) headers.push(colMap.id);
    if (hasCol(colMap.fileId)) headers.push(colMap.fileId);
    if (hasCol(colMap.name)) headers.push(colMap.name);
    if (hasCol(colMap.mimeType)) headers.push(colMap.mimeType);
    if (hasCol("source_mime_type")) headers.push("source_mime_type");
    if (hasCol("url")) headers.push("url");
    if (hasCol(colMap.size)) headers.push(colMap.size);
    if (hasCol(colMap.data)) headers.push(colMap.data);
    if (shouldEmbed && hasCol(colMap.embedding)) headers.push(colMap.embedding);
    if (hasCol(colMap.updatedAt)) headers.push(colMap.updatedAt);

    // Check if table has a dedicated 'metadata' column
    const hasMetadataCol = hasCol("metadata");
    if (hasMetadataCol && !headers.includes("metadata")) {
      headers.push("metadata");
    }

    // Collect any extra custom metadata keys that exist as dedicated columns in the table
    const customColKeys = new Set();
    blobEntries.forEach((entry) => {
      const meta = isBlob_(entry) ? {} : (entry.metadata || {});
      if (meta && typeof meta === "object") {
        Object.keys(meta).forEach((k) => {
          if (!headers.includes(k) && hasCol(k)) {
            customColKeys.add(k);
          }
        });
      }
    });
    customColKeys.forEach((k) => headers.push(k));

    const rows = [headers];
    const nowIso = new Date();

    blobEntries.forEach((entry, idx) => {
      const b = isBlob_(entry) ? entry : entry.blob;
      if (!isBlob_(b)) {
        throw new Error(`Invalid Blob at entry index ${idx}. Expected a Google Apps Script Blob.`);
      }
      const meta = isBlob_(entry) ? {} : (entry.metadata || {});
      const rowId = meta[colMap.id] !== undefined
        ? meta[colMap.id]
        : (meta.id !== undefined
            ? meta.id
            : ((typeof Utilities !== "undefined" && typeof Utilities.getUuid === "function")
                ? Utilities.getUuid()
                : `blob_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`));
      const fileId = meta[colMap.fileId] !== undefined
        ? meta[colMap.fileId]
        : (meta.file_id !== undefined ? meta.file_id : (meta.fileId || ""));
      const fileName = meta[colMap.name] || meta.name || b.getName() || `blob_${idx + 1}`;
      const mimeType = meta[colMap.mimeType] || meta.mime_type || b.getContentType() || "application/octet-stream";
      const byteSize = (meta[colMap.size] !== undefined && meta[colMap.size] !== null)
        ? Number(meta[colMap.size])
        : ((meta.size !== undefined && meta.size !== null)
            ? Number(meta.size)
            : (b ? b.getBytes().length : 0));
      const updatedAt = meta[colMap.updatedAt] || meta.updated_at || nowIso;

      let embedding = null;
      if (shouldEmbed && hasCol(colMap.embedding)) {
        try {
          const textToEmbed = (entry && entry.embeddingText)
            || (options && options.embeddingOptions && options.embeddingOptions.text)
            || (options && options.embeddingText)
            || (meta && (meta.description || meta.caption || meta.summary || meta.name))
            || null;
          const embedOpts = Object.assign({}, options.embeddingOptions || {});
          if (textToEmbed) {
            embedOpts.text = textToEmbed;
          }
          embedding = getGeminiEmbedding_(textToEmbed || b, apiKey, embedOpts);
        } catch (e) {
          console.warn(`Embedding generation skipped for Blob '${fileName}': ${e.message}`);
          embedding = null;
        }
      }

      const rowMap = {
        [colMap.id]: rowId,
        [colMap.fileId]: fileId,
        [colMap.name]: fileName,
        [colMap.mimeType]: mimeType,
        [colMap.size]: byteSize,
        [colMap.data]: (b && b.getBytes().length > 0) ? b : null,
        [colMap.updatedAt]: updatedAt,
      };
      if (hasCol("source_mime_type") && meta.source_mime_type !== undefined) {
        rowMap["source_mime_type"] = meta.source_mime_type;
      }
      if (hasCol("url") && meta.url !== undefined) {
        rowMap["url"] = meta.url;
      }
      if (shouldEmbed && hasCol(colMap.embedding)) {
        rowMap[colMap.embedding] = embedding;
      }

      // Merge custom metadata into dedicated columns if they exist
      customColKeys.forEach((k) => {
        if (meta[k] !== undefined) {
          rowMap[k] = meta[k];
        }
      });

      // Bundle any remaining unmapped metadata into the 'metadata' JSON column
      if (hasMetadataCol) {
        const extraMeta = {};
        Object.keys(meta).forEach((k) => {
          if (!headers.includes(k) && k !== "metadata") {
            extraMeta[k] = meta[k];
          }
        });
        if (Object.keys(extraMeta).length > 0) {
          rowMap["metadata"] = JSON.stringify(extraMeta);
        } else if (meta["metadata"] !== undefined) {
          rowMap["metadata"] = typeof meta["metadata"] === "object" ? JSON.stringify(meta["metadata"]) : String(meta["metadata"]);
        } else {
          rowMap["metadata"] = null;
        }
      }

      const rowData = headers.map((h) => (rowMap[h] !== undefined ? rowMap[h] : null));
      rows.push(rowData);
    });

    return this.insertValues(rows);
  }

  /**
   * Imports a single Google Drive file into the Iceberg table by its File ID.
   * Preserves Google Drive File ID in metadata, and converts Google Docs/Sheets/Slides
   * to PDF (default) or a specified target MIME type (e.g. 'text/plain', 'text/csv', 'text/html').
   *
   * @param {string} fileId The Google Drive File ID.
   * @param {Object} [metadata={}] Optional additional user-defined metadata columns.
   * @param {Object} [options={}] Configuration options.
   * @param {string} [options.targetMimeType] Target conversion MIME type (e.g. 'application/pdf', 'text/plain', 'text/csv').
   * @param {Object} [options.mimeTypeMap] Map of source Google Docs MIME types to target MIME types.
   * @param {boolean} [options.embed] Whether to compute Gemini embedding.
   * @param {Object} [options.columnMap] Column mapping object.
   * @return {{success: boolean, fileId: string, name: string, mimeType: string, originalMimeType: string, size: number}} Summary of inserted file.
   */
  insertDriveFile(fileId, metadata = {}, options = {}) {
    if (!fileId || typeof fileId !== "string" || fileId.trim() === "") {
      throw new Error("Parameter 'fileId' must be a non-empty string.");
    }
    if (typeof DriveApp === "undefined") {
      throw new Error("Google DriveApp service is required to import Google Drive files.");
    }

    const cleanFileId = fileId.trim();
    const file = DriveApp.getFileById(cleanFileId);
    const sourceMime = file.getMimeType();
    const isGoogleWorkspaceDoc = sourceMime.startsWith("application/vnd.google-apps.");
    let blob;

    // Resolve target conversion MIME type
    let desiredMime = options.targetMimeType || options.convertToMimeType || null;
    if (!desiredMime && options.mimeTypeMap && options.mimeTypeMap[sourceMime]) {
      desiredMime = options.mimeTypeMap[sourceMime];
    }

    if (desiredMime && typeof desiredMime === "string") {
      const targetLower = desiredMime.trim().toLowerCase();
      // Special handling: DriveApp.getAs('text/plain') is not supported for Google Docs.
      // We extract plain text via DocumentApp to cleanly support text/plain conversion.
      if (sourceMime === "application/vnd.google-apps.document" && targetLower === "text/plain") {
        try {
          let text = "";
          if (typeof DocumentApp !== "undefined" && typeof DocumentApp.openById === "function") {
            text = DocumentApp.openById(cleanFileId).getBody().getText();
          }
          blob = Utilities.newBlob(text, "text/plain", `${file.getName()}.txt`);
        } catch (e) {
          blob = file.getBlob();
        }
      } else {
        try {
          blob = file.getAs(desiredMime.trim());
        } catch (err) {
          // Fallback to default getBlob() (PDF export for Google Workspace items)
          blob = file.getBlob();
        }
      }
    } else {
      // Default: Google Workspace files (Docs, Sheets, Slides) natively convert to PDF via getBlob()
      blob = file.getBlob();
    }

    // Explicitly verify and confirm converted MIME type
    let verifiedMime = blob.getContentType();
    if (!verifiedMime || verifiedMime === "application/octet-stream") {
      verifiedMime = isGoogleWorkspaceDoc ? "application/pdf" : (file.getMimeType() || "application/octet-stream");
      blob.setContentType(verifiedMime);
    }

    // Harmonize file name extension with converted MIME type
    let blobName = blob.getName() || file.getName() || `doc_${cleanFileId}`;
    if (verifiedMime === "application/pdf" && !blobName.toLowerCase().endsWith(".pdf")) {
      blobName = `${blobName}.pdf`;
      blob.setName(blobName);
    } else if (verifiedMime === "text/plain" && !blobName.toLowerCase().endsWith(".txt")) {
      blobName = `${blobName}.txt`;
      blob.setName(blobName);
    }

    // Extract text from Google Docs for high-quality Gemini embeddings if applicable
    let docText = null;
    if (sourceMime === "application/vnd.google-apps.document") {
      try {
        if (typeof DocumentApp !== "undefined" && typeof DocumentApp.openById === "function") {
          docText = DocumentApp.openById(cleanFileId).getBody().getText();
        }
      } catch (e) {}
    }

    const combinedMeta = Object.assign({
      file_id: cleanFileId,
      name: blobName,
      mime_type: verifiedMime,
      source_mime_type: sourceMime,
      url: file.getUrl(),
    }, metadata);

    const fileOptions = Object.assign({}, options);
    if (docText && (!fileOptions.embeddingOptions || !fileOptions.embeddingOptions.text)) {
      fileOptions.embeddingOptions = Object.assign({}, fileOptions.embeddingOptions, { text: docText });
    }

    this.insertBlob(blob, combinedMeta, fileOptions);

    return {
      success: true,
      fileId: cleanFileId,
      name: blobName,
      mimeType: verifiedMime,
      originalMimeType: sourceMime,
      size: blob.getBytes().length,
    };
  }

  /**
   * Imports all files from a Google Drive folder (and subfolders) into the Iceberg table.
   *
   * @param {string} folderId The Google Drive Folder ID.
   * @param {Object} [options={}] Configuration options.
   * @param {boolean} [options.recursive=true] Whether to traverse subfolders recursively.
   * @param {string} [options.targetMimeType] Target conversion MIME type (e.g. 'application/pdf', 'text/plain').
   * @param {Object} [options.mimeTypeMap] Map of source MIME types to target MIME types.
   * @param {Array<string>|function(GoogleAppsScript.Drive.File):boolean} [options.mimeTypeFilter] Filter for files to include.
   * @param {number} [options.batchSize=20] Number of files to batch insert per query.
   * @param {boolean} [options.embed] Whether to compute Gemini embedding.
   * @param {Object} [options.metadata={}] Additional metadata common to all imported files.
   * @return {{success: boolean, totalFiles: number, insertedRows: number, files: Array<Object>}} Execution summary.
   */
  insertDriveFolder(folderId, options = {}) {
    if (!folderId || typeof folderId !== "string" || folderId.trim() === "") {
      throw new Error("Parameter 'folderId' must be a non-empty string.");
    }
    if (typeof DriveApp === "undefined") {
      throw new Error("Google DriveApp service is required to import Google Drive files.");
    }

    const cleanFolderId = folderId.trim();
    const folder = DriveApp.getFolderById(cleanFolderId);
    const recursive = options.recursive !== false;
    const batchSize = Math.max(1, Number(options.batchSize) || 20);

    const collectedFiles = collectDriveFilesFromFolder_(folder, recursive, options.mimeTypeFilter);
    if (collectedFiles.length === 0) {
      return { success: true, totalFiles: 0, insertedRows: 0, files: [] };
    }

    let totalInserted = 0;
    const fileSummaries = [];

    for (let i = 0; i < collectedFiles.length; i += batchSize) {
      const chunk = collectedFiles.slice(i, i + batchSize);
      const entries = [];

      chunk.forEach((file) => {
        const sourceMime = file.getMimeType();
        let desiredMime = options.targetMimeType || options.convertToMimeType || null;
        if (!desiredMime && options.mimeTypeMap && options.mimeTypeMap[sourceMime]) {
          desiredMime = options.mimeTypeMap[sourceMime];
        }

        let blob;
        if (desiredMime && typeof desiredMime === "string") {
          try {
            blob = file.getAs(desiredMime.trim());
          } catch (e) {
            blob = file.getBlob();
          }
        } else {
          blob = file.getBlob();
        }

        if (!blob.getName()) {
          blob.setName(file.getName());
        }

        const meta = Object.assign({
          file_id: file.getId(),
          name: file.getName(),
          mime_type: blob.getContentType(),
          source_mime_type: sourceMime,
          url: file.getUrl(),
        }, options.metadata || {});

        entries.push({ blob: blob, metadata: meta });
        fileSummaries.push({
          fileId: file.getId(),
          name: file.getName(),
          mimeType: blob.getContentType(),
          originalMimeType: sourceMime,
          size: blob.getBytes().length,
        });
      });

      const count = this.insertBlobs(entries, options);
      totalInserted += count;
    }

    return {
      success: true,
      totalFiles: collectedFiles.length,
      insertedRows: totalInserted,
      files: fileSummaries,
    };
  }

  /**
   * Executes atomic DML update on rows matching the predicate.
   *
   * @param {string} setClause e.g. "price = price * 1.1, stock = 10"
   * @param {string} whereClause e.g. "id = 101"
   * @return {string} Status message.
   */
  update(setClause, whereClause) {
    if (!setClause || typeof setClause !== "string" || setClause.trim() === "") {
      throw new Error("Parameter 'setClause' must be a non-empty string.");
    }
    if (!whereClause || typeof whereClause !== "string" || whereClause.trim() === "") {
      throw new Error("Parameter 'whereClause' must be a non-empty string.");
    }
    if (/;\s*$/m.test(setClause) || /;\s*$/m.test(whereClause)) {
      throw new Error("Semicolons are prohibited in DML statements.");
    }
    const cleanWhere = whereClause.trim().replace(/^WHERE\s+/i, "");
    const sql = `
      UPDATE ${this.getFullPath()}
      SET ${setClause.trim()}
      WHERE ${cleanWhere};
    `;
    runBqJob_(this.projectId, sql, this.location);
    return `Table ${this.tableName} successfully updated.`;
  }

  /**
   * Deletes rows satisfying the predicate.
   *
   * @param {string} whereClause Filter condition. Mandatory to prevent accidental truncation.
   * @return {string} Status message.
   */
  deleteRows(whereClause) {
    if (!whereClause || typeof whereClause !== "string" || whereClause.trim() === "") {
      throw new Error("Parameter 'whereClause' must be a non-empty string.");
    }
    if (/;\s*$/m.test(whereClause)) {
      throw new Error("Semicolons are prohibited in DML statements.");
    }
    const cleanWhere = whereClause.trim().replace(/^WHERE\s+/i, "");
    const sql = `
      DELETE FROM ${this.getFullPath()}
      WHERE ${cleanWhere};
    `;
    runBqJob_(this.projectId, sql, this.location);
    return `Rows deleted matching (${cleanWhere}).`;
  }

  /**
   * Exports queried table records directly into a Google Spreadsheet.
   * Automatically expands sheet rows and columns if current sheet bounds are exceeded.
   *
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet Target sheet.
   * @param {string} [startA1="A1"] Top-left cell range.
   * @param {Object} [filter={}] Optional filter parameters.
   * @return {number} Total rows written (including header).
   */
  exportToSheet(sheet, startA1 = "A1", filter = {}) {
    if (!sheet || typeof sheet.getRange !== "function") {
      throw new Error("A valid Google Apps Script Sheet object is required.");
    }
    const values = this.getValues(filter);
    if (!values || values.length === 0 || !values[0] || values[0].length === 0) {
      return 0;
    }

    const rowCount = values.length;
    const colCount = values[0].length;
    const cell = sheet.getRange(startA1 || "A1");
    const startRow = cell.getRow();
    const startCol = cell.getColumn();

    // Auto-expand sheet bounds if required
    const maxRows = sheet.getMaxRows();
    const maxCols = sheet.getMaxColumns();
    const neededRows = startRow + rowCount - 1;
    const neededCols = startCol + colCount - 1;

    if (neededRows > maxRows) {
      sheet.insertRowsAfter(maxRows, neededRows - maxRows);
    }
    if (neededCols > maxCols) {
      sheet.insertColumnsAfter(maxCols, neededCols - maxCols);
    }

    sheet.getRange(startRow, startCol, rowCount, colCount).setValues(values);
    return rowCount;
  }

  /**
   * Drops the table entity from the catalog.
   *
   * @param {boolean} [ifExists=false] Whether to append IF EXISTS.
   * @return {string} Status message.
   */
  remove(ifExists = false) {
    const ifExistsClause = ifExists ? "IF EXISTS " : "";
    const sql = `DROP TABLE ${ifExistsClause}${this.getFullPath()};`;
    runBqJob_(this.projectId, sql, this.location);
    return `Table ${this.tableName} was successfully dropped.`;
  }
}

/* -------------------------------------------------------------------------- */
/*                               INTERNAL HELPERS                             */
/* -------------------------------------------------------------------------- */

/**
 * Ensures BigQuery dataset exists in the designated location.
 * Idempotent: checks if dataset exists, and provisions it automatically if not found.
 *
 * @private
 * @param {string} projectId Google Cloud Project ID.
 * @param {string} datasetId BigQuery Dataset / Catalog ID.
 * @param {string} location Google Cloud Region.
 * @param {string} [description] Optional dataset description.
 * @return {string} Verified or created dataset location.
 */
function ensureBigQueryDataset_(projectId, datasetId, location, description) {
  if (typeof BigQuery === "undefined" || !BigQuery.Datasets) {
    return location;
  }
  try {
    const ds = BigQuery.Datasets.get(projectId, datasetId);
    return ds.location;
  } catch (e) {
    const errMsg = e.message || String(e);
    if (errMsg.indexOf("Not found") !== -1 || errMsg.indexOf("not found") !== -1 || errMsg.indexOf("404") !== -1) {
      console.log(`⚡ Catalog Dataset [${datasetId}] absent in [${projectId}]. Automatically provisioning at [${location}]...`);
      const resource = {
        datasetReference: {
          projectId: projectId,
          datasetId: datasetId,
        },
        location: location,
      };
      if (description) {
        resource.description = description;
      }
      const created = BigQuery.Datasets.insert(resource, projectId);
      console.log(`✅ Catalog Dataset [${datasetId}] created successfully at [${created.location}].`);
      return created.location;
    }
    throw new Error(`Catalog dataset provision failed for [${datasetId}]: ${errMsg}`);
  }
}

/**
 * Runs a query and returns result rows and affected row count.
 * @private
 * @param {string} projectId
 * @param {string} query
 * @param {string} location
 * @param {number} [maxWaitMs=120000]
 * @return {{rows: Array<Object>, affectedRows: number}}
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

  // Protocol 17 Strict Error Check
  if (queryResults.errors && queryResults.errors.length > 0) {
    const errDetails = queryResults.errors.map((e) => e.message).join("; ");
    throw new Error(`BigQuery job execution failed [${jobId}]: ${errDetails}`);
  }

  const affectedRows = queryResults.numDmlAffectedRows !== undefined
    ? Number(queryResults.numDmlAffectedRows)
    : 0;

  const fields = queryResults.schema ? queryResults.schema.fields.map((f) => f.name) : [];
  let allRows = queryResults.rows || [];

  // Handle pagination if results span multiple pages (>10,000 rows)
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
 * Runs a query and returns values as a 2D array (first row = headers).
 * @private
 * @param {string} projectId
 * @param {string} query
 * @param {string} location
 * @param {number} [maxWaitMs=120000]
 * @return {Array<Array<any>>}
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

  // Protocol 17 Strict Error Check
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

/**
 * Formats a JavaScript value into a valid BigQuery Standard SQL literal expression.
 * Safely handles null, numbers (including NaN/Infinity), booleans, strings, Dates,
 * Google Apps Script Blobs (via FROM_BASE64), Arrays (ARRAY literals), and generic objects.
 *
 * @private
 * @param {any} val Input value.
 * @param {number} [rowNum=1] Row index for descriptive error messages.
 * @return {string} SQL literal string.
 */
function formatSqlValue_(val, rowNum = 1) {
  if (val === null || val === undefined) return "NULL";
  if (typeof val === "number") {
    if (Number.isNaN(val)) return "CAST('NaN' AS FLOAT64)";
    if (val === Infinity) return "CAST('+Infinity' AS FLOAT64)";
    if (val === -Infinity) return "CAST('-Infinity' AS FLOAT64)";
    return String(val);
  }
  if (typeof val === "boolean") return val ? "TRUE" : "FALSE";
  if (typeof val === "string") {
    const safe = val.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    return `'${safe}'`;
  }
  if (val instanceof Date) {
    if (isNaN(val.getTime())) throw new Error(`Invalid Date at row ${rowNum}`);
    return `TIMESTAMP('${val.toISOString()}')`;
  }
  if (isBlob_(val)) {
    const bytes = val.getBytes();
    const base64Bytes = (typeof Utilities !== "undefined" && typeof Utilities.base64Encode === "function")
      ? Utilities.base64Encode(bytes)
      : Buffer.from(bytes).toString("base64");
    return `FROM_BASE64('${base64Bytes}')`;
  }
  if (Array.isArray(val)) {
    const elems = val.map((elem) => formatSqlValue_(elem, rowNum)).join(", ");
    return `[${elems}]`;
  }
  const jsonSafe = JSON.stringify(val).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  return `'${jsonSafe}'`;
}

/**
 * Checks whether a given value is a Google Apps Script Blob object.
 *
 * @private
 * @param {any} val
 * @return {boolean}
 */
function isBlob_(val) {
  return Boolean(
    val &&
    typeof val === "object" &&
    typeof val.getBytes === "function" &&
    typeof val.getContentType === "function"
  );
}

/**
 * Retrieves the Gemini API Key from ScriptProperties or UserProperties.
 * Returns null if not configured or if PropertiesService is unavailable.
 *
 * @private
 * @return {string|null}
 */
function getGeminiApiKey_() {
  try {
    if (typeof PropertiesService === "undefined" || !PropertiesService) return null;
    let key = null;
    if (PropertiesService.getScriptProperties) {
      key = PropertiesService.getScriptProperties().getProperty("GEMINI_API_KEY");
    }
    if (!key && PropertiesService.getUserProperties) {
      key = PropertiesService.getUserProperties().getProperty("GEMINI_API_KEY");
    }
    return (key && typeof key === "string" && key.trim() !== "") ? key.trim() : null;
  } catch (e) {
    return null;
  }
}

/**
 * Calls the Gemini Embedding API to generate vector embeddings for text or binary Blobs.
 * Supports text strings, text blobs, and multimodal image/document blobs.
 *
 * @private
 * @param {string|GoogleAppsScript.Base.Blob} content Text string or binary Blob.
 * @param {string} apiKey Valid Gemini API key.
 * @param {Object} [options={}] Optional parameters (e.g. { model: 'text-embedding-004' }).
 * @return {Array<number>} Vector of floats.
 */
function getGeminiEmbedding_(content, apiKey, options = {}) {
  if (!apiKey || typeof apiKey !== "string" || apiKey.trim() === "") {
    throw new Error("A valid GEMINI_API_KEY is required to generate embeddings.");
  }

  // 1. If explicit text override is provided in options, use it
  if (options.text && typeof options.text === "string") {
    if (options.text.trim() === "") return null;
    content = options.text.trim();
  }

  // 2. If content is string
  if (typeof content === "string") {
    if (content.trim() === "") return null;
  }

  // 3. If content is Blob
  if (isBlob_(content)) {
    const mimeType = content.getContentType() || "application/octet-stream";
    if (mimeType.startsWith("text/") || mimeType === "application/json" || mimeType === "application/javascript") {
      const textData = (typeof content.getDataAsString === "function")
        ? content.getDataAsString()
        : Buffer.from(content.getBytes()).toString("utf8");
      if (!textData || textData.trim() === "") return null;
      content = textData;
    } else {
      // Non-text binary formats (e.g. image, PDF, zip).
      // Gemini's embedContent endpoint only accepts text parts.
      // Return null gracefully without making a failing HTTP call.
      return null;
    }
  }

  const primaryModel = options.model || (isBlob_(content) && !content.getContentType().startsWith("text/") ? "gemini-embedding-001" : "text-embedding-004");
  const fallbackModel = (primaryModel === "text-embedding-004") ? "gemini-embedding-001" : "text-embedding-004";
  const modelsToTry = [primaryModel, fallbackModel];

  let lastError = null;
  for (const model of modelsToTry) {
    let requestBody;
    if (isBlob_(content)) {
      const mimeType = content.getContentType() || "application/octet-stream";
      const bytes = content.getBytes();
      const base64Data = (typeof Utilities !== "undefined" && typeof Utilities.base64Encode === "function")
        ? Utilities.base64Encode(bytes)
        : Buffer.from(bytes).toString("base64");

      if (mimeType.startsWith("text/")) {
        const textData = (typeof content.getDataAsString === "function")
          ? content.getDataAsString()
          : Buffer.from(bytes).toString("utf8");
        requestBody = {
          model: `models/${model}`,
          content: { parts: [{ text: textData }] }
        };
      } else {
        requestBody = {
          model: `models/${model}`,
          content: {
            parts: [
              {
                inlineData: {
                  mimeType: mimeType,
                  data: base64Data
                }
              }
            ]
          }
        };
      }
    } else {
      requestBody = {
        model: `models/${model}`,
        content: {
          parts: [{ text: String(content) }]
        }
      };
    }

    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${model}:embedContent?key=${encodeURIComponent(apiKey.trim())}`;
    const response = UrlFetchApp.fetch(endpoint, {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(requestBody),
      muteHttpExceptions: true
    });

    const statusCode = response.getResponseCode();
    const respText = response.getContentText();

    if (statusCode >= 200 && statusCode < 300) {
      const json = JSON.parse(respText);
      if (json.embedding && Array.isArray(json.embedding.values)) {
        return json.embedding.values;
      }
      throw new Error(`Unexpected Gemini Embedding API response format: ${respText}`);
    }

    lastError = new Error(`Gemini Embedding API call failed for model '${model}' [HTTP ${statusCode}]: ${respText}`);
    // Only continue to fallback if 404 (model unavailable)
    if (statusCode !== 404) {
      throw lastError;
    }
  }

  throw lastError;
}

/**
 * Recursively collects files from a Google Drive folder.
 *
 * @private
 * @param {GoogleAppsScript.Drive.Folder} folder Folder instance.
 * @param {boolean} recursive Whether to traverse subfolders.
 * @param {Array<string>|function(GoogleAppsScript.Drive.File):boolean} [filter] Optional MIME filter or predicate.
 * @return {Array<GoogleAppsScript.Drive.File>} List of collected files.
 */
function collectDriveFilesFromFolder_(folder, recursive, filter) {
  const files = [];

  function matchesFilter(file) {
    if (!filter) return true;
    if (typeof filter === "function") {
      return Boolean(filter(file));
    }
    if (Array.isArray(filter)) {
      return filter.includes(file.getMimeType());
    }
    return true;
  }

  function traverse(curFolder) {
    const fileIter = curFolder.getFiles();
    while (fileIter.hasNext()) {
      const file = fileIter.next();
      if (matchesFilter(file)) {
        files.push(file);
      }
    }
    if (recursive) {
      const folderIter = curFolder.getFolders();
      while (folderIter.hasNext()) {
        traverse(folderIter.next());
      }
    }
  }

  traverse(folder);
  return files;
}

/**
 * Uploads a binary Blob directly to Google Drive using Drive API v3 multipart/form-data.
 * Supports files up to 50MB with custom metadata.
 * Based on Kanshi Tanaike's multipart upload architecture for Google Apps Script.
 * Reference: https://gist.githubusercontent.com/tanaikech/5cd0dc9ea7d75e4a2ff65049ed3d78c3/raw/13b25563d38f9e8809bff7d5835dd727d91a968a/submit.md
 *
 * @private
 * @param {GoogleAppsScript.Base.Blob} blob Binary file Blob.
 * @param {Object} [metadata={}] File metadata (name, mimeType, parents, description, etc.).
 * @return {{id: string, name: string, mimeType: string, size: number, webViewLink: string, url: string}} Drive File summary.
 */
function uploadToDriveMultipart_(blob, metadata = {}) {
  if (!isBlob_(blob)) {
    throw new Error("uploadToDriveMultipart_ requires a valid Google Apps Script Blob.");
  }

  const allowedDriveFields = new Set([
    "name", "description", "mimeType", "parents", "starred", "properties",
    "appProperties", "folderColorRgb", "viewedByMeTime", "writersCanShare",
    "copyRequiresWriterPermission", "originalFilename", "contentHints"
  ]);

  const driveMeta = {
    name: metadata.name || blob.getName() || `file_${Date.now()}`,
    mimeType: metadata.targetMimeType || metadata.mimeType || blob.getContentType() || "application/octet-stream",
  };

  const customProps = {};
  Object.keys(metadata).forEach((k) => {
    if (k === "targetMimeType" || k === "name" || k === "mimeType") return;
    if (allowedDriveFields.has(k)) {
      driveMeta[k] = metadata[k];
    } else if (metadata[k] !== undefined && metadata[k] !== null) {
      customProps[k] = typeof metadata[k] === "object" ? JSON.stringify(metadata[k]) : String(metadata[k]);
    }
  });

  if (Object.keys(customProps).length > 0 && !driveMeta.properties) {
    driveMeta.properties = customProps;
  }

  const payload = {
    metadata: Utilities.newBlob(JSON.stringify(driveMeta), "application/json"),
    file: blob,
  };

  const endpoint = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=" +
    encodeURIComponent("id,name,mimeType,size,webViewLink,webContentLink");

  const response = UrlFetchApp.fetch(endpoint, {
    method: "post",
    payload: payload,
    headers: {
      Authorization: "Bearer " + ScriptApp.getOAuthToken(),
    },
    muteHttpExceptions: true,
  });

  const statusCode = response.getResponseCode();
  const respText = response.getContentText();

  if (statusCode < 200 || statusCode >= 300) {
    throw new Error(`Drive API v3 Multipart upload failed [HTTP ${statusCode}]: ${respText}`);
  }

  const result = JSON.parse(respText);
  return {
    id: result.id,
    name: result.name,
    mimeType: result.mimeType,
    size: Number(result.size) || blob.getBytes().length,
    webViewLink: result.webViewLink || `https://drive.google.com/open?id=${result.id}`,
    url: result.webViewLink || `https://drive.google.com/open?id=${result.id}`,
  };
}

/**
 * Uploads a binary Blob directly to Google Cloud Storage (GCS).
 * Supports files up to 50MB (UrlFetchApp limit).
 *
 * @private
 * @param {GoogleAppsScript.Base.Blob} blob Binary file Blob.
 * @param {string} bucketName Target GCS Bucket name.
 * @param {string} objectPath Target GCS object name/path (e.g. "assets/photo.jpg").
 * @param {Object} [customMetadata={}] Optional custom metadata attributes.
 * @return {{bucket: string, name: string, uri: string, size: number, md5Hash: string, contentType: string}}
 */
function uploadToGcsMultipart_(blob, bucketName, objectPath, customMetadata = {}) {
  if (!isBlob_(blob)) {
    throw new Error("uploadToGcsMultipart_ requires a valid Google Apps Script Blob.");
  }
  if (!bucketName || typeof bucketName !== "string" || bucketName.trim() === "") {
    throw new Error("GCS Bucket name must be a non-empty string.");
  }

  const cleanBucket = bucketName.trim().replace(/^gs:\/\//, "").split("/")[0];
  const cleanPath = objectPath ? String(objectPath).trim().replace(/^\//, "") : (blob.getName() || `asset_${Date.now()}`);

  const endpoint = `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(cleanBucket)}/o?uploadType=media&name=${encodeURIComponent(cleanPath)}`;

  const response = UrlFetchApp.fetch(endpoint, {
    method: "post",
    contentType: blob.getContentType() || "application/octet-stream",
    payload: blob.getBytes(),
    headers: {
      Authorization: "Bearer " + ScriptApp.getOAuthToken(),
    },
    muteHttpExceptions: true,
  });

  const statusCode = response.getResponseCode();
  const respText = response.getContentText();

  if (statusCode < 200 || statusCode >= 300) {
    throw new Error(`GCS Upload failed for gs://${cleanBucket}/${cleanPath} [HTTP ${statusCode}]: ${respText}`);
  }

  const result = JSON.parse(respText);
  return {
    bucket: result.bucket || cleanBucket,
    name: result.name || cleanPath,
    uri: `gs://${result.bucket || cleanBucket}/${result.name || cleanPath}`,
    size: Number(result.size) || blob.getBytes().length,
    md5Hash: result.md5Hash || "",
    contentType: result.contentType || blob.getContentType(),
    mediaLink: result.mediaLink || "",
  };
}


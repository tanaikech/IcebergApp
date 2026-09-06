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
   * Creates a new Apache Iceberg / BigLake table with specified schema.
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

    if (clusterCols.length > 0) {
      const uniqueClusterCols = [...new Set(clusterCols)].slice(0, 4);
      clusterClause = `\nCLUSTER BY ${uniqueClusterCols.map((c) => `\`${String(c).trim().replace(/`/g, "")}\``).join(", ")}`;
    }

    // Connection and Iceberg Table Options
    // BigQuery requires WITH CONNECTION clause when table_format = 'ICEBERG' is present.
    let connectionClause = "";
    let optionsClause = "";

    if (options.connection && typeof options.connection === "string" && options.connection.trim() !== "") {
      const conn = options.connection.trim();
      const formattedConn = conn.toUpperCase() === "DEFAULT"
        ? "DEFAULT"
        : (conn.includes("/") ? `\`${conn.replace(/`/g, "")}\`` : `\`${this.projectId}.${this.location}.${conn.replace(/`/g, "")}\``);
      
      connectionClause = `\nWITH CONNECTION ${formattedConn}`;

      const tableOptions = [`table_format = 'ICEBERG'`];
      if (options.storageUri && typeof options.storageUri === "string" && options.storageUri.trim() !== "") {
        const cleanUri = options.storageUri.trim().replace(/'/g, "\\'");
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
    });
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
   * Internal query runner.
   *
   * @private
   * @param {string} query SQL statement.
   * @return {{rows: Array<Object>, affectedRows: number}} Result set and execution metadata.
   */
  _runQuery(query) {
    return runBqJob_(this.projectId, query, this.location);
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
    /** @private @type {Date|null} */
    this.timeTravelSnapshot = null;
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
      ${limitClause};
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
      const escaped = row.map((val) => {
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
          if (isNaN(val.getTime())) throw new Error(`Invalid Date in data row ${rIdx + 1}`);
          return `TIMESTAMP('${val.toISOString()}')`;
        }
        const jsonSafe = JSON.stringify(val).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        return `'${jsonSafe}'`;
      }).join(", ");

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
  const headers = queryResults.schema.fields.map((f) => f.name);

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

  const data = allRows.map((row) => row.f.map((cell) => cell.v));
  return [headers, ...data];
}

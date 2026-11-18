# Model Context Protocol (MCP) Server

`dlt-doctor` includes a native, zero-dependency [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) stdio server. It exposes `dlt-doctor`'s static analysis, Catalyst AST verification, DBU cost forecasting, and Delta compaction engines directly to AI coding assistants like **Claude Desktop**, **Cursor**, **VS Code (Cline / Roo Code)**, and custom LLM agents.

With MCP enabled, your AI assistant can evaluate PySpark and Databricks SQL code locally before execution, flag silent-drop anti-patterns, recommend Liquid Clustering keys, and size compute pools without burning Databricks cluster DBUs.

---

## Architecture & Zero Dependencies

The `dlt-doctor` MCP server communicates over standard input/output (`stdio`) using the JSON-RPC 2.0 protocol (`2024-11-05` specification). It requires no external network calls, no heavyweight telemetry daemons, and zero third-party dependencies outside the standard Python library and `dlt-doctor` core modules.

```
+-------------------------------------------------------------+
| LLM Host (Claude Desktop / Cursor / Roo Code / Custom Agent) |
+-------------------------------------------------------------+
                              |
                     stdio (JSON-RPC 2.0)
                              |
                              v
+-------------------------------------------------------------+
|                     dlt-doctor mcp                          |
|  +-------------------------------------------------------+  |
|  | AST Analyzer | PlanDoctor | CostDoctor | Compaction   |  |
|  +-------------------------------------------------------+  |
+-------------------------------------------------------------+
```

---

## Available Tools

The MCP server exposes five specialized diagnostic tools:

### 1. `analyze_pyspark_source`
Statically audits PySpark or Delta Live Tables Python code for Catalyst plan bloat, silent drop rules, skew risks, and JVM-Python socket serialization.

- **Parameters:**
  - `source_code` *(string, required)*: The Python source code of the pipeline or transformation.
  - `file_name` *(string, optional)*: Filename context for diagnostics (default: `pipeline.py`).
- **Returns:** Structured JSON audit scorecard listing rule violations (`DD001` - `DD015`), severity, affected line numbers, offending AST constructs, and recommended remediation code.

### 2. `analyze_sql_source`
Statically audits Databricks SQL or DLT SQL queries for quality expectation risks, Cartesian products, and schema evolution anti-patterns.

- **Parameters:**
  - `sql_code` *(string, required)*: The Databricks SQL query text.
- **Returns:** Structured JSON report identifying expectation drop risks, cross joins, and syntax recommendations.

### 3. `scan_pipeline_directory`
Recursively scans a local directory of PySpark scripts, SQL files, and Databricks notebooks (`.ipynb`), producing an aggregated repository health report.

- **Parameters:**
  - `directory_path` *(string, required)*: Absolute or relative local path to the pipeline directory.
  - `recursive` *(boolean, optional)*: Whether to traverse nested subdirectories (default: `true`).
- **Returns:** Repository-wide audit summary containing total files scanned, total findings by severity (`CRITICAL`, `HIGH`, `MEDIUM`, `LOW`), and per-file rule breaches.

### 4. `forecast_dbu_cost`
Forecasts Databricks Unit (DBU) consumption and evaluates Serverless vs. Classic compute costs based on ingestion velocity and transformation complexity.

- **Parameters:**
  - `files_per_hour` *(integer, required)*: Estimated incoming file arrival rate.
  - `avg_file_mb` *(number, optional)*: Average file size in megabytes (default: `10.0`).
  - `is_continuous_streaming` *(boolean, optional)*: `true` for continuous streaming; `false` for triggered batch (default: `true`).
  - `active_hours_per_day` *(number, optional)*: Operational hours per day (default: `24.0`).
  - `shuffle_join_count` *(integer, optional)*: Number of wide shuffle stages or joins (default: `1`).
- **Returns:** Projected monthly DBU consumption, recommended worker types and counts, and an architectural recommendation (Serverless vs. Classic Compute pool).

### 5. `audit_compaction_debt`
Evaluates Delta Lake Parquet file size distributions to detect small-file fragmentation debt and synthesizes targeted SQL optimization commands.

- **Parameters:**
  - `table_name` *(string, required)*: Target table name (e.g., `prod_lakehouse.silver.orders`).
  - `file_sizes_bytes` *(array of integers, required)*: Array of Parquet data file sizes in bytes extracted from Delta log or file storage.
- **Returns:** Fragmentation score, small-file count (< 10 MB), average file size, and actionable `OPTIMIZE` and `TBLPROPERTIES` statements.

---

## Available Resources

The server exposes dynamic system resources for agent context priming:

- **URI**: `dlt-doctor://rules`
- **MIME Type**: `application/json`
- **Description**: The complete registry of all `dlt-doctor` static rules (`DD001` - `DD015`, `PLAN-001` - `PLAN-004`, `UC-001` - `UC-004`), including severity levels, descriptions, and rationale documentation.

---

## Configuration & Installation

### Option A: 1-Click Installation via Smithery

If you use [Smithery](https://smithery.ai/), install `dlt-doctor` into Claude Desktop with a single command:

```bash
npx -y @smithery/cli install dlt-doctor --client claude
```

### Option B: Claude Desktop Configuration

Add the `dlt-doctor` server to your `claude_desktop_config.json`:

=== "macOS"
    ```json
    {
      "mcpServers": {
        "dlt-doctor": {
          "command": "dlt-doctor",
          "args": ["mcp"]
        }
      }
    }
    ```
    *File location: `~/Library/Application Support/Claude/claude_desktop_config.json`*

=== "Windows"
    ```json
    {
      "mcpServers": {
        "dlt-doctor": {
          "command": "dlt-doctor.exe",
          "args": ["mcp"]
        }
      }
    }
    ```
    *File location: `%APPDATA%\\Claude\\claude_desktop_config.json`*

=== "Linux"
    ```json
    {
      "mcpServers": {
        "dlt-doctor": {
          "command": "dlt-doctor",
          "args": ["mcp"]
        }
      }
    }
    ```
    *File location: `~/.config/Claude/claude_desktop_config.json`*

### Option C: Cursor IDE Configuration

Configure the MCP server in Cursor under **Settings > Features > MCP**:

1. Click **+ Add New MCP Server**.
2. Fill in:
   - **Name**: `dlt-doctor`
   - **Type**: `command`
   - **Command**: `dlt-doctor mcp`

Alternatively, add it to your project's `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "dlt-doctor": {
      "command": "dlt-doctor",
      "args": ["mcp"]
    }
  }
}
```

### Option D: Manual stdio Verification

You can verify the stdio transport directly using `python -m dlt_doctor mcp` or `dlt-doctor mcp`:

```bash
echo '{"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}' | dlt-doctor mcp
```

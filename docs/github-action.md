# GitHub Action Integration

Publishing and pull-request validation for `dlt-doctor` is available on the **[GitHub Marketplace](https://github.com/marketplace/actions/dlt-doctor)**.

## Pull Request Quality Gate

Add `.github/workflows/pyspark-audit.yml` to your repository:

```yaml
name: PySpark Quality Gate

on:
  pull_request:
    branches: [main]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout PR
        uses: actions/checkout@v4

      - name: Run dlt-doctor Quality Gate
        uses: MsMansiDhruv/dlt-doctor@v0.4.1
        with:
          path: "src/pipelines"
          format: "text"
```

## Inputs

| Input | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `path` | **Yes** | `"src"` | Path to Python file (`.py`), notebook (`.ipynb`), or directory of pipelines. |
| `format` | No | `"text"` | Output format: `text`, `markdown`, or `html`. |

## Fail-Safe Build Gates

- If any scanned file introduces a **CRITICAL** severity regression (e.g., row-level Python `@udf` that breaks Whole-Stage CodeGen, unparseable syntax), the GitHub Action fails with exit code `1`, halting the pull request.
- If issues are **HIGH** or **MEDIUM** (e.g. chained `.withColumn()` cascades), the action prints full remediation advice and DBU cost savings while allowing CI to succeed.

# Generated for dlt-doctor MCP server
FROM python:3.11-slim

WORKDIR /app

# Install package
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# Run MCP stdio server
ENTRYPOINT ["dlt-doctor", "mcp"]

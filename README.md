# MinIO Model-Context Protocol (MCP)

This project implements a Model-Context Protocol (MCP) server and client for MinIO object storage. It provides a standardized way to interact with MinIO.

## Features

### Server

#### Resources

Exposes MinIO data through **Resources**. The server can access and provide:
- Text files (automatically detected based on file extension)
- Binary files (handled as application/octet-stream)
- Bucket contents (up to 1000 objects per bucket)

#### Tools

* **ListBuckets**
  * Returns a list of all buckets owned by the authenticated sender of the request
  * Optional parameters: `start_after` (pagination), `max_buckets` (limit results)

* **ListObjects**
  * Returns some or all (up to 1,000) of the objects in a bucket with each request
  * Required parameter: `bucket_name`
  * Optional parameters: `prefix` (filter by prefix), `max_keys` (limit results)

* **GetObject**
  * Retrieves an object from MinIO
  * Required parameters: `bucket_name`, `object_name`

* **PutObject**
  * Uploads a file to MinIO bucket using fput method
  * Required parameters: `bucket_name`, `object_name`, `file_path`

### Client

The project includes multiple client implementations:

1. **Basic Client** - Simple client for direct interaction with the MinIO MCP server
2. **Anthropic Client** - Integration with Anthropic's Claude models for AI-powered interactions with MinIO

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/minio-mcp.git
cd minio-mcp
```

2. Install dependencies using pip:
```bash
pip install -r requirements.txt
```

Or using uv:
```bash
uv pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file in the root directory with the following configuration:

```env
# MinIO Configuration
MINIO_ENDPOINT=play.min.io
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_SECURE=true
MINIO_MAX_BUCKETS=5
MINIO_MAX_KEYS=1000

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8000

# For Anthropic Client (if using)
ANTHROPIC_API_KEY=your_anthropic_api_key
```

## Usage

### Running the Server

Start the server over stdio (the default transport) with:

```bash
python src/minio_mcp_server/server.py
```

To expose the server over HTTP using Server-Sent Events (SSE) via
`fastmcp`, choose the `sse` transport. The host and port default to the
values from `SERVER_HOST`/`SERVER_PORT` (or `0.0.0.0:8000`), and the
endpoints default to `/sse` for the event stream and `/messages/` for
message posts:

```bash
python src/minio_mcp_server/server.py --transport sse --host 0.0.0.0 --port 8000
```

Set `MCP_TRANSPORT` to `sse`, `streamable-http`, or `stdio` in your
environment to change the default transport without supplying command
line flags. Optional `SERVER_SSE_PATH` and `SERVER_MESSAGE_PATH`
variables let you override the SSE endpoints if you need to align with a
different MCP client configuration.

### Using the Basic Client

```python
from src.client import main
import asyncio

asyncio.run(main())
```

### Using the Anthropic Client

1. Configure the servers in `src/client/servers_config.json`:

```json
{
  "mcpServers": {
    "minio_service": {
      "command": "python",
      "args": ["path/to/minio_mcp_server/server.py"]
    }
  }
}
```

2. Run the client:

```bash
python src/client/mcp_anthropic_client.py
```

3. Interact with the assistant:
   - The assistant will automatically detect available tools
   - You can ask questions about your MinIO data
   - The assistant will use the appropriate tools to retrieve information

4. Exit the session:
   - Type `quit` or `exit` to end the session

## Integration with Claude Desktop

You can integrate this MCP server with Claude Desktop:

### Configuration

On MacOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`  
On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "minio-mcp": {
      "command": "python",
      "args": [
        "path/to/minio-mcp/src/minio_mcp_server/server.py"
      ]
    }
  }
}
```

## Development

### Project Structure

```
minio-mcp/
├── src/
│   ├── client/                  # Client implementations
│   │   ├── mcp_anthropic_client.py  # Anthropic integration
│   │   └── servers_config.json  # Server configuration
│   ├── minio_mcp_server/        # MCP server implementation
│   │   ├── resources/           # Resource implementations
│   │   │   └── minio_resource.py  # MinIO resource
│   │   └── server.py            # Main server implementation
│   ├── __init__.py
│   └── client.py                # Basic client implementation
├── LICENSE
├── pyproject.toml
├── README.md
└── requirements.txt
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
black src/
isort src/
flake8 src/
```

## Debugging

Since MCP servers run over stdio, debugging can be challenging. For the best debugging experience, we recommend using the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector python path/to/minio-mcp/src/minio_mcp_server/server.py
```

Upon launching, the Inspector will display a URL that you can access in your browser to begin debugging.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
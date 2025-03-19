# FlinkMPC

## AI Agent for Confluent Cloud Flink 

compatible with Cursor or Claude Desktop as a MCP Server

![image](https://github.com/user-attachments/assets/d18dabde-aee8-484b-84b7-420c552cd0ee)

Features:
- get list of all statements in some environment
- get details of a statement
- get list of compute pools in some environment
- get details of a compute pool

Requirements
- python
- uv
- Cursor or Claude Desktop

### mcp.json example in Cursor

```
{
  "mcpServers": {
    "FlinkMCP": {
        "command": "/Users/jansvoboda/.local/bin/uv",
        "args": [
            "--directory",
            "/Users/jansvoboda/Code/python/FlinkMCP",
            "run",
            "server.py"
        ]
    }
  }
}
```

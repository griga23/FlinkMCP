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

Examples:

![image](https://github.com/user-attachments/assets/90f8f92b-35f6-4b5f-a815-94f0877c45fb)

![image](https://github.com/user-attachments/assets/478fa786-7079-41fb-b4f1-b94b099c8a62)


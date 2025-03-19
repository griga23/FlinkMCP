from typing import Any
import httpx
import base64
import json
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("FlinkMCP")

# Constants
ORG_ID="" 
CLOUD_PROVIDER="aws"
CLOUD_REGION="eu-central-1"
CLIENT_PRINCIPAL_ID=""

FLINK_API_KEY=""
FLINK_API_SECRET=""
CCLOUD_API_KEY = ""
CCLOUD_API_SECRET = ""

BASE64_FLINK_KEY = base64.b64encode(f"{FLINK_API_KEY}:{FLINK_API_SECRET}".encode()).decode()
BASE64_CCLOUD_KEY = base64.b64encode(f"{CCLOUD_API_KEY}:{CCLOUD_API_SECRET}".encode()).decode()
FLINK_CCLOUD_URL=f"https://flink.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud/sql/v1/organizations/{ORG_ID}/environments"


async def make_ccloud_request(url: str, headers: dict[str, str] = None) -> dict[str, Any] | None:
    """Make a get request to the Confluent Cloud API with proper error handling."""

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error making request: {str(e)}")
            return None

async def make_ccloud_post_request(url: str, headers: dict[str, str] = None, data: dict[str, Any] = None) -> dict[str, Any] | None:
    """Make a post request to the Confluent Cloud API with proper error handling."""

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, data=data, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error making request: {str(e)}")
            return None

def format_statement(statement: str) -> str:
    """Format a statement into a readable string."""
    statement = json.loads(statement)
    metadata = statement["metadata"]
    spec = statement["spec"]
    status = statement["status"]
    
    # Build the base output
    output = f"""
Statement Name: {statement.get('name', 'Unknown')}
Environment ID: {statement.get('environment_id', 'Unknown')}
Organization ID: {statement.get('organization_id', 'Unknown')}
Created At: {metadata.get('created_at', 'Unknown')}
Updated At: {metadata.get('updated_at', 'Unknown')}
Status: {status.get('phase', 'Unknown')}
Network Kind: {status.get('network_kind', 'Unknown')}
SQL Kind: {status.get('traits', {}).get('sql_kind', 'Unknown')}
Is Append Only: {status.get('traits', {}).get('is_append_only', 'Unknown')}
Is Bounded: {status.get('traits', {}).get('is_bounded', 'Unknown')}
SQL Statement: {spec.get('statement', 'No statement available')}
Status Detail: {status.get('detail', 'No details available')}
Compute Pool ID: {spec.get('compute_pool_id', 'Unknown')}
Principal: {spec.get('principal', 'Unknown')}"""

    # Add properties if available
    properties = spec.get('properties', {})
    if properties:
        output += "\nProperties:"
        for key, value in properties.items():
            output += f"\n  - {key}: {value}"

    # Add scaling status if available
    scaling_status = status.get('scaling_status', {})
    if scaling_status:
        output += f"\nScaling Status: {scaling_status.get('scaling_state', 'Unknown')}"
        output += f"\nLast Updated: {scaling_status.get('last_updated', 'Unknown')}"

    # Add latest offsets if available
    latest_offsets = status.get('latest_offsets', {})
    if latest_offsets:
        output += f"\nLatest Offsets: {latest_offsets}"
        output += f"\nOffsets Timestamp: {status.get('latest_offsets_timestamp', 'Unknown')}"

    # Add schema information if available
    schema = status.get('traits', {}).get('schema', {})
    if schema and 'columns' in schema:
        output += "\nSchema:"
        for column in schema['columns']:
            col_type = column['type']
            type_str = col_type['type']
            if 'length' in col_type:
                type_str += f"({col_type['length']})"
            if col_type.get('nullable', True):
                type_str += " NULL"
            else:
                type_str += " NOT NULL"
            output += f"\n  - {column['name']}: {type_str}"

    return output


def format_compute_pool(compute_pool: str) -> str:
    """Format a compute pool into a readable string."""
    compute_pool = json.loads(compute_pool)
    metadata = compute_pool["metadata"]
    spec = compute_pool["spec"] 
    status = compute_pool["status"]

    output = f"""
Kind: {compute_pool.get('kind', 'Unknown')}
Compute Pool Name: {spec.get('display_name', 'Unknown')}
ID: {compute_pool.get('id', 'Unknown')}
Created At: {metadata.get('created_at', 'Unknown')}
Updated At: {metadata.get('updated_at', 'Unknown')}
Resource Name: {metadata.get('resource_name', 'Unknown')}
Status: {status.get('phase', 'Unknown')}
Current CFU: {status.get('current_cfu', 'Unknown')}
Max CFU: {spec.get('max_cfu', 'Unknown')}
Cloud: {spec.get('cloud', 'Unknown')}
Region: {spec.get('region', 'Unknown')}
Environment ID: {spec.get('environment', {}).get('id', 'Unknown')}
AI Enabled: {spec.get('enable_ai', 'Unknown')}"""

    return output

@mcp.tool()
async def get_environments() -> list[str]:
    """Get all environments from Confluent Cloud."""
    headers = {
        "Authorization": f"Basic {BASE64_CCLOUD_KEY}",
        "Host": "api.confluent.cloud",
        "Accept": "application/json"
    }
    
    url = f"https://api.confluent.cloud/org/v2/environments"
    data = await make_ccloud_request(url, headers)
    
    if not data:
        return []
        
    return [env["id"] for env in data.get("data", [])]

@mcp.tool()
async def get_all_statements() -> str:
    """Get all Flink statements from all environments."""
    environments = await get_environments()
    
    if not environments:
        return "Unable to fetch environments or no environments found."
    
    all_statements = []
    for env in environments:
        headers = {
            "Authorization": f"Basic {BASE64_FLINK_KEY}",
            "Host": f"flink.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud",
            "Accept": "application/json"
        }
        url = f"{FLINK_CCLOUD_URL}/{env}/statements"
        data = await make_ccloud_request(url, headers)
        
        if data and "data" in data:
            statements = data.get("data", [])
            for statement in statements:
                formatted = format_statement(statement)
                all_statements.append(f"Environment: {env}\n{formatted}")
    
    return "\n---\n".join(all_statements) if all_statements else "No statements found in any environment"

@mcp.tool()
async def get_statements(env: str) -> str:
    """Get all statements for an environment in Confluent Cloud.

    Args:
        env: The environment to get statements for.
    """
    headers = {
        "Authorization": f"Basic {BASE64_FLINK_KEY}",
        "Host": f"flink.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud",
        "Accept": "application/json"
    }
    url = f"{FLINK_CCLOUD_URL}/{env}/statements"
    data = await make_ccloud_request(url, headers)

    if not data:
        return f"Unable to fetch statements or no statements found. URL: {url}"

    statements = [format_statement(json.dumps(statement)) for statement in data.get("data", [])]
    return "\n---\n".join(statements) if statements else "No statements found"

@mcp.tool()
async def get_statement(env: str, statement: str) -> str:
    """Get details about a Flink statement.

    Args:
        env: The environment to get statements for.
        statement: The statement to get details for.
    """
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{FLINK_API_KEY}:{FLINK_API_SECRET}'.encode()).decode()}",
        "Host": f"flink.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud",
        "Accept": "application/json"
    }
    url = f"{FLINK_CCLOUD_URL}/{env}/statements/{statement}"
    data = await make_ccloud_request(url, headers)

    if not data:
        return f"Unable to fetch statement or statement not found. URL: {url}"

    return format_statement(json.dumps(data))


@mcp.tool()
async def get_pools(env: str) -> str:
    """Get all Flink compute pools for an environment in Confluent Cloud.

    Args:
        env: The environment to get pools for.
    """
    headers = {
        "Authorization": f"Basic {BASE64_CCLOUD_KEY}",
        "Host": "confluent.cloud",
        "Accept": "application/json"
    }
    
    url = f"https://confluent.cloud/api/fcpm/v2/compute-pools?environment={env}"
    data = await make_ccloud_request(url, headers)

    if not data:
        return f"Unable to fetch compute pools or no compute pools found. URL: {url}"

    pools = [format_compute_pool(json.dumps(pool)) for pool in data.get("data", [])]
    return "\n---\n".join(pools) if pools else "No Compute Pools found"

@mcp.tool()
async def get_pool(env: str, pool: str) -> str:
    """Get an information about a Flink compute pool in Confluent Cloud.

    Args:
        env: The environment to get pools for.
        pool: The pool to get information for.
    """
    headers = {
        "Authorization": f"Basic {BASE64_CCLOUD_KEY}",
        "Host": "confluent.cloud",
        "Accept": "application/json"
    }
    url = f"https://confluent.cloud/api/fcpm/v2/compute-pools/{pool}?environment={env}"
    data = await make_ccloud_request(url, headers)

    if not data:
        return f"Unable to fetch compute pool or compute pool not found. URL: {url}"
        
    return format_compute_pool(json.dumps(data))


@mcp.tool()
async def submit_statement(env: str, statement: str, compute_pool_id: str) -> str:
    """Submit a Flink statement.

    Args:
        env: The environment to submit statement to.
        statement: The Flink SQL statement to submit.
    """
    url = f"{FLINK_CCLOUD_URL}/{env}/statements/{statement}"
    headers = {
        "Authorization": f"Basic {BASE64_FLINK_KEY}",
        "Host": f"https://flink.{CLOUD_REGION}.{CLOUD_PROVIDER}.confluent.cloud",
        "Accept": "application/json"
    }
    data = {
        "name": statement,
        "organization_id": ORG_ID,
        "environment_id": env,
        "spec": {
            "statement": statement,
            "compute_pool_id": compute_pool_id,
            "principal": CLIENT_PRINCIPAL_ID,
            "stopped": False
        }
    }

    data = await make_ccloud_post_request(url, headers, data)

    if not data:
        return f"Unable to fetch statement or statement not found. URL: {url}"

    statement_data = data.get("data", {})
    if statement_data:
        return format_statement(statement_data)
    else:
        return "No statement found"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
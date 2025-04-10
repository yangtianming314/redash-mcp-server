#!/usr/bin/env python
from fastapi import FastAPI
from modelcontextprotocol.sdk.server import Server
from modelcontextprotocol.sdk.server.stdio import StdioServerTransport
from modelcontextprotocol.sdk.types import (
    CallToolRequestSchema,
    ListToolsRequestSchema,
    McpError,
    ErrorCode
)
import os
import requests
import json
from typing import Optional
from pydantic import BaseModel
from .tools.execute_query import QueryExecutor

app = FastAPI()

class RedashConfig(BaseModel):
    url: str
    api_key: str

@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

class RedashMCPServer:
    def __init__(self):
        self.server = Server(
            {"name": "redash-mcp", "version": "1.0.0"},
            {"capabilities": {"resources": {}, "tools": {}}}
        )
        self.setup_tools()
        
    def setup_tools(self):
        @self.server.set_request_handler(ListToolsRequestSchema)
        async def list_tools(request):
            return {
                "tools": [
                    {
                        "name": "execute_query",
                        "description": "Execute Redash query and get results",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query_id": {"type": "integer"},
                                "params": {"type": "object"}
                            },
                            "required": ["query_id"]
                        }
                    }
                ]
            }

        @self.server.set_request_handler(CallToolRequestSchema)
        async def call_tool(request):
            if request.params.name == "execute_query":
                return await self.execute_query(request.params.arguments)
            raise McpError(ErrorCode.MethodNotFound, "Unknown tool")

    async def execute_query(self, args):
        try:
            config = RedashConfig(
                url=os.getenv("REDASH_URL"),
                api_key=os.getenv("REDASH_API_KEY")
            )
            if not config.url or not config.api_key:
                raise McpError(ErrorCode.InvalidConfiguration, 
                             "Redash URL and API key must be configured")
            
            executor = QueryExecutor(config.url, config.api_key)
            results = await executor.execute(
                args.get("query_id"),
                args.get("params", {})
            )
            
            return {
                "content": [{
                    "type": "application/json",
                    "text": json.dumps(results)
                }]
            }
        except McpError as e:
            raise
        except Exception as e:
            raise McpError(ErrorCode.InternalError, str(e))

async def run():
    server = RedashMCPServer()
    transport = StdioServerTransport()
    await server.server.connect(transport)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())

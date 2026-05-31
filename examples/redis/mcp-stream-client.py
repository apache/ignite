#!/usr/bin/env python3
"""
MCP Streamable HTTP 客户端示例
使用 mcp 客户端的 streamable_http 传输方式
"""

import asyncio
import json
from typing import Optional, Dict, Any, List
from contextlib import AsyncExitStack

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

class MCPStreamableHTTPClient:
    """MCP Streamable HTTP 客户端"""
    
    def __init__(self, server_url: str = "http://localhost:8787/mcp"):
        """
        初始化 Streamable HTTP 客户端
        
        Args:
            server_url: MCP 服务器地址（支持 SSE 流式响应）
        """
        self.server_url = server_url
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        
    async def connect(self):
        """连接到 MCP 服务器（使用 streamable http）"""
        print(f"\n🔌 Connecting to MCP server via streamable HTTP...")
        print(f"   Server URL: {self.server_url}")
        
        # 创建 streamable http 客户端
        # streamablehttp_client 返回 (read_stream, write_stream, transport)
        self.streams_context = streamable_http_client(self.server_url)
        read_stream, write_stream, transport = await self.exit_stack.enter_async_context(
            self.streams_context
        )
        
        # 创建客户端会话
        self.session = await self.exit_stack.enter_async_context(
            ClientSession(read_stream, write_stream)
        )
        
        # 初始化连接
        result = await self.session.initialize()
        
        print(f"✅ Connected successfully!")
        print(f"   Protocol version: {result.protocolVersion}")
        print(f"   Server capabilities: {result.capabilities}")
        
        return result
    
    async def list_tools(self) -> List[Dict[str, Any]]:
        """获取服务器支持的工具列表"""
        if not self.session:
            raise RuntimeError("Not connected to server. Call connect() first.")
        
        print("\n" + "="*70)
        print("📋 Fetching available tools...")
        print("="*70)
        
        # 调用 tools/list 方法
        response = await self.session.list_tools()
        
        tools = []
        for tool in response.tools:
            tool_info = {
                "name": tool.name,
                "description": tool.description or "No description",
                "parameters": tool.inputSchema if hasattr(tool, 'inputSchema') else {}
            }
            tools.append(tool_info)
            
            print(f"\n🔧 Tool: {tool.name}")
            print(f"   📝 Description: {tool_info['description'][:100]}")
            
            if 'properties' in tool_info['parameters']:
                params = tool_info['parameters']['properties']
                required = tool_info['parameters'].get('required', [])
                print(f"   📊 Parameters:")
                for param_name, param_info in params.items():
                    required_mark = " *required" if param_name in required else ""
                    param_type = param_info.get('type', 'unknown')
                    param_desc = param_info.get('description', '')
                    print(f"      - {param_name} ({param_type}){required_mark}: {param_desc}")
        
        print(f"\n✅ Total tools available: {len(tools)}")
        return tools
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        调用工具（支持流式和非流式）
        
        Args:
            tool_name: 工具名称
            arguments: 工具参数
            
        Returns:
            工具执行结果
        """
        if not self.session:
            raise RuntimeError("Not connected to server")
        
        print(f"\n" + "="*70)
        print(f"🔨 Calling tool: {tool_name}")
        print(f"📦 Arguments: {json.dumps(arguments, indent=2)}")
        print("="*70)
        
        # 调用工具 - streamable http 会自动处理流式响应
        result = await self.session.call_tool(tool_name, arguments)
        
        print(f"\n✅ Result:")
        
        # 处理返回的内容
        for content in result.content:
            if hasattr(content, 'text'):
                try:
                    data = json.loads(content.text)
                    print(json.dumps(data, indent=2))
                except json.JSONDecodeError:
                    print(content.text)
            elif hasattr(content, 'data'):
                print(f"Binary data: {len(content.data)} bytes")
        
        return {"result": result.content[0].text if result.content else None}
    
    async def call_tool_with_streaming(self, tool_name: str, arguments: Dict[str, Any]):
        """
        调用流式工具并实时处理响应
        
        Streamable HTTP 会自动处理流式响应，这里演示如何处理逐步到达的数据
        """
        if not self.session:
            raise RuntimeError("Not connected to server")
        
        print(f"\n" + "="*70)
        print(f"🌊 Calling streaming tool: {tool_name}")
        print(f"📦 Arguments: {json.dumps(arguments, indent=2)}")
        print("="*70)
        print("\n📡 Streaming response:\n")
        
        try:
            # Streamable HTTP 会自动处理流式响应
            # 如果服务器返回流式响应，结果会逐步到达
            result = await self.session.call_tool(tool_name, arguments)
            
            # 处理流式结果
            for content in result.content:
                if hasattr(content, 'text'):
                    # 尝试解析 JSON
                    try:
                        data = json.loads(content.text)
                        
                        # 根据消息类型处理
                        if isinstance(data, dict):
                            msg_type = data.get('type')
                            
                            if msg_type == 'progress':
                                progress = data.get('progress', 0)
                                message = data.get('message', '')
                                # 使用 \r 实现进度条效果
                                print(f"\r   📊 Progress: {progress}% - {message}", end='')
                                
                            elif msg_type == 'chunk':
                                chunk_data = data.get('data', {})
                                chunk_seq = data.get('sequence', 0)
                                print(f"\n   📦 Chunk #{chunk_seq}:")
                                print(f"      {json.dumps(chunk_data, indent=2)}")
                                
                            elif msg_type == 'metadata':
                                metadata = data.get('data', {})
                                print(f"\n   📋 Metadata: {json.dumps(metadata, indent=2)}")
                                
                            elif msg_type == 'complete':
                                print(f"\n   ✅ Complete!")
                                final_result = data.get('result', {})
                                if final_result:
                                    print(f"   Final result: {json.dumps(final_result, indent=2)}")
                                    
                            elif msg_type == 'error':
                                print(f"\n   ❌ Error: {data.get('message', 'Unknown error')}")
                                
                            else:
                                print(f"\n   📝 {json.dumps(data, indent=2)}")
                        else:
                            print(f"\n   📝 {content.text}")
                            
                    except json.JSONDecodeError:
                        print(f"\n   📝 {content.text}")
                        
                elif hasattr(content, 'data'):
                    print(f"   📦 Binary chunk: {len(content.data)} bytes")
            
            print("\n" + "="*70)
            print("✅ Streaming completed")
            print("="*70)
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            raise
    
    async def list_resources(self) -> List[Dict[str, Any]]:
        """获取资源列表（如果服务器支持）"""
        if not self.session:
            raise RuntimeError("Not connected to server")
        
        try:
            response = await self.session.list_resources()
            resources = []
            for resource in response.resources:
                resource_info = {
                    "uri": resource.uri,
                    "name": resource.name,
                    "description": resource.description,
                    "mimeType": getattr(resource, 'mimeType', None)
                }
                resources.append(resource_info)
                print(f"\n📄 Resource: {resource.name}")
                print(f"   URI: {resource.uri}")
                print(f"   Description: {resource.description}")
            return resources
        except Exception as e:
            print(f"⚠️  Resources not supported: {e}")
            return []
    
    async def read_resource(self, uri: str) -> str:
        """读取资源内容"""
        if not self.session:
            raise RuntimeError("Not connected to server")
        
        try:
            result = await self.session.read_resource(uri)
            for content in result.contents:
                if hasattr(content, 'text'):
                    return content.text
            return ""
        except Exception as e:
            print(f"❌ Error reading resource: {e}")
            raise
    
    async def close(self):
        """关闭连接"""
        await self.exit_stack.aclose()
        print("\n🔌 Connection closed")

async def demo_basic_usage():
    """基础使用演示"""
    print("\n" + "="*70)
    print("🚀 MCP Streamable HTTP Client Demo - Basic Usage")
    print("="*70)
    
    client = MCPStreamableHTTPClient("http://localhost:8787/mcp")
    
    try:
        # 1. 连接服务器
        await client.connect()
        
        # 2. 获取工具列表
        tools = await client.list_tools()
        
        if not tools:
            print("⚠️  No tools available")
            return
        
        # 3. 调用普通工具
        if any(t['name'] == 'get_weather' for t in tools):
            await client.call_tool("get_weather", {
                "city": "Shanghai",
                "date": "2024-01-15"
            })
        
        # 4. 调用流式工具
        if any(t['name'] == 'stream_file' for t in tools):
            await client.call_tool_with_streaming("stream_file", {
                "filePath": "/tmp/toy_train.py",
                "batchSize": 100,
                "encoding": "UTF-8"
            })
        
        # 5. 调用另一个流式工具
        if any(t['name'] == 'process_data' for t in tools):
            await client.call_tool_with_streaming("process_data", {
                "dataset": "large_dataset",
                "batchSize": 1000,
                "streamMode": True
            })
        
    finally:
        await client.close()

async def demo_interactive():
    """交互式演示"""
    print("\n" + "="*70)
    print("🎮 MCP Streamable HTTP Client Demo - Interactive Mode")
    print("="*70)
    
    client = MCPStreamableHTTPClient("http://localhost:8080/mcp")
    
    try:
        # 连接服务器
        await client.connect()
        
        # 获取工具列表
        tools = await client.list_tools()
        
        if not tools:
            print("⚠️  No tools available")
            return
        
        while True:
            print("\n" + "="*70)
            print("📌 Interactive Menu")
            print("="*70)
            
            # 显示工具列表
            print("\nAvailable tools:")
            for i, tool in enumerate(tools, 1):
                print(f"  {i:2d}. {tool['name']} - {tool['description'][:50]}")
            print(f"  {len(tools) + 1:2d}. List resources")
            print(f"  {len(tools) + 2:2d}. Exit")
            
            try:
                choice = input(f"\nSelect option (1-{len(tools) + 2}): ").strip()
                
                if not choice:
                    continue
                    
                choice_num = int(choice)
                
                if choice_num == len(tools) + 2:
                    print("👋 Goodbye!")
                    break
                elif choice_num == len(tools) + 1:
                    await client.list_resources()
                    continue
                elif 1 <= choice_num <= len(tools):
                    tool = tools[choice_num - 1]
                    
                    print(f"\n🔧 Selected tool: {tool['name']}")
                    print(f"📝 Description: {tool['description']}")
                    
                    # 显示参数
                    params = tool['parameters'].get('properties', {})
                    required = tool['parameters'].get('required', [])
                    
                    if params:
                        print("\n📊 Parameters:")
                        for param_name, param_info in params.items():
                            required_mark = " *required" if param_name in required else ""
                            param_type = param_info.get('type', 'unknown')
                            param_desc = param_info.get('description', '')
                            print(f"  - {param_name} ({param_type}){required_mark}: {param_desc}")
                    
                    # 输入参数
                    arguments = {}
                    for param_name in params:
                        default = params[param_name].get('default', '')
                        prompt = f"Enter {param_name} [{default}]: " if default else f"Enter {param_name}: "
                        value = input(prompt).strip()
                        
                        if not value and default:
                            value = default
                        
                        if value or param_name in required:
                            # 尝试解析 JSON 或数字
                            try:
                                arguments[param_name] = json.loads(value)
                            except:
                                # 尝试转换为数字
                                if value.isdigit():
                                    arguments[param_name] = int(value)
                                elif value.replace('.', '').isdigit():
                                    arguments[param_name] = float(value)
                                else:
                                    arguments[param_name] = value
                    
                    # 选择调用模式
                    stream_mode = input("\nUse streaming mode? (y/n) [y]: ").strip().lower()
                    stream_mode = stream_mode != 'n'  # 默认使用流式
                    
                    if stream_mode:
                        await client.call_tool_with_streaming(tool['name'], arguments)
                    else:
                        await client.call_tool(tool['name'], arguments)
                    
                    input("\nPress Enter to continue...")
                else:
                    print("❌ Invalid choice")
                    
            except ValueError:
                print("❌ Invalid input, please enter a number")
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()

async def demo_resource_operations():
    """资源操作演示"""
    print("\n" + "="*70)
    print("📚 MCP Streamable HTTP Client Demo - Resource Operations")
    print("="*70)
    
    client = MCPStreamableHTTPClient("http://localhost:8080/mcp")
    
    try:
        # 连接服务器
        await client.connect()
        
        # 列出资源
        resources = await client.list_resources()
        
        if resources:
            # 读取第一个资源
            first_resource = resources[0]
            print(f"\n📖 Reading resource: {first_resource['name']}")
            content = await client.read_resource(first_resource['uri'])
            print(f"Content preview: {content[:200]}...")
        else:
            print("⚠️  No resources available")
            
    finally:
        await client.close()

async def main():
    """主函数"""
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "interactive":
            await demo_interactive()
        elif mode == "resources":
            await demo_resource_operations()
        elif mode == "basic":
            await demo_basic_usage()
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python client.py [basic|interactive|resources]")
    else:
        # 默认运行基础演示
        await demo_basic_usage()

if __name__ == "__main__":
    asyncio.run(main())
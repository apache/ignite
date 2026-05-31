package io.vertx.webmvc.mcp;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

public class McpSchema {

    // 工具调用请求参数（Map结构）
    @Data
    public static class ToolCallParams {
        private String name;
        private Map<String, Object> arguments; // 参数为Map类型
        private Map<String, Object> clientInfo;
        private String sessionId;
    }

    // MCP请求模型
    @Data
    public static class McpRequest {
        private String jsonrpc = "2.0";
        private String id;
        private String method;
        private ToolCallParams params; // 工具调用参数

    }

    // 工具调用结果类
    @Data
    public static class McpToolCallResult {
        private List<Map<String, Object>> content;
        private boolean isError;
        private Long executionTime;

    }

    // MCP响应模型
    @Data
    public static class McpResponse {
        private String jsonrpc = "2.0";
        private String id;
        private Object result;
        private McpError error;
    }

    @Data
    @AllArgsConstructor
    public static class McpError extends Exception {
        private int code;
        private String message;
        private Object data;
    }

    // 服务器能力类
    @Data
    public static class ServerCapabilities {
        private String protocolVersion;
        private Map<String, Object> tools;
        private Map<String, Object> streaming;
        private Map<String, Object> resources;


        public Map<String, Object> toMap() {
            Map<String, Object> map = new java.util.HashMap<>();
            if (tools != null) map.put("tools", tools);
            if (streaming != null) map.put("streaming", streaming);
            if (resources != null) map.put("resources", resources);
            return map;
        }
    }
}

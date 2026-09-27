package io.vertx.webmvc.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

public class McpSchema {

    /**
     * JSON Schema 定义（符合 MCP 规范）
     * MCP 使用 JSON Schema 的子集来定义工具参数
     */
    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class JSONSchema {

        /**
         * 数据类型: object, array, string, number, integer, boolean, null
         */
        @JsonProperty("type")
        private String type;

        /**
         * 对象类型的属性定义
         */
        @JsonProperty("properties")
        private Map<String, JSONSchema> properties;

        /**
         * 必需属性列表
         */
        @JsonProperty("required")
        private List<String> required;

        /**
         * 属性描述
         */
        @JsonProperty("description")
        private String description;

        /**
         * 枚举值
         */
        @JsonProperty("enum")
        private List<Object> enumValues;

        /**
         * 默认值
         */
        @JsonProperty("default")
        private Object defaultValue;

        /**
         * 字符串格式: date-time, email, hostname, ipv4, ipv6, uri, uuid 等
         */
        @JsonProperty("format")
        private String format;

        /**
         * 字符串最小长度
         */
        @JsonProperty("minLength")
        private Integer minLength;

        /**
         * 字符串最大长度
         */
        @JsonProperty("maxLength")
        private Integer maxLength;

        /**
         * 数字最小值
         */
        @JsonProperty("minimum")
        private Number minimum;

        /**
         * 数字最大值
         */
        @JsonProperty("maximum")
        private Number maximum;

        /**
         * 数组项的类型定义
         */
        @JsonProperty("items")
        private JSONSchema items;

        /**
         * 数组最小项数
         */
        @JsonProperty("minItems")
        private Integer minItems;

        /**
         * 数组最大项数
         */
        @JsonProperty("maxItems")
        private Integer maxItems;

        /**
         * 是否允许额外属性
         */
        @JsonProperty("additionalProperties")
        private Boolean additionalProperties = true;
    }

    /**
     * MCP 工具元数据（扩展字段）
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class ToolMeta {

        /**
         * 是否支持流式响应
         */
        @JsonProperty("isStreaming")
        private Boolean isStreaming;

        /**
         * 工具执行超时时间（毫秒）
         */
        @JsonProperty("timeout")
        private Long timeout;

        /**
         * 工具进度回调支持
         */
        @JsonProperty("supportsProgress")
        private Boolean supportsProgress;

        /**
         * 工具版本
         */
        @JsonProperty("version")
        private String version;

        /**
         * 工具作者/来源
         */
        @JsonProperty("author")
        private String author;

        public ToolMeta() {}

        public ToolMeta(Boolean isStreaming) {
            this.isStreaming = isStreaming;
        }
    }

    /**
     * MCP 工具定义
     * 参考: https://spec.modelcontextprotocol.io/specification/2025-03-26/server/tools/
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class McpToolInfo {

        /**
         * 工具的唯一标识符
         * 必须：是
         */
        @JsonProperty("name")
        private String name;

        /**
         * 工具的人类可读描述
         * 必须：否（但强烈推荐）
         */
        @JsonProperty("description")
        private String description;

        /**
         * 工具的输入参数 schema（JSON Schema 格式）
         */
        @JsonProperty("inputSchema")
        private JSONSchema inputSchema;

        @JsonProperty("outputSchema")
        private JSONSchema outputSchema;

        /**
         * MCP 协议扩展字段
         */
        @JsonProperty("meta")
        private ToolMeta meta;

        // 构造函数
        public McpToolInfo() {}

        public McpToolInfo(String name, String description, JSONSchema inputSchema) {
            this.name = name;
            this.description = description;
            this.inputSchema = inputSchema;
        }
    }

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
        private Map<String, Object> structuredContent;
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

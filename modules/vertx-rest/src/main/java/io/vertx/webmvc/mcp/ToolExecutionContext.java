package io.vertx.webmvc.mcp;

import io.vertx.ext.web.RoutingContext;
import lombok.Getter;

import java.util.Map;

/**
 * 工具执行上下文
 */
public class ToolExecutionContext {
    // Getters
    @Getter
    private final String requestId;
    @Getter
    private final String toolName;
    @Getter
    private final Map<String, Object> arguments;
    @Getter
    private final RoutingContext routingContext;
    @Getter
    private final SessionManager.McpSession session;

    public ToolExecutionContext(String requestId, String toolName, Map<String, Object> arguments,
                                RoutingContext routingContext, SessionManager.McpSession session) {
        this.requestId = requestId;
        this.toolName = toolName;
        this.arguments = arguments;
        this.routingContext = routingContext;
        this.session = session;
    }
}
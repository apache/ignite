package io.vertx.webmvc.mcp;

import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 工具执行上下文
 */
public class ToolExecutionContext {
    // Getters
    @Getter
    private final String requestId;
    @Getter
    private final ToolExecutor tool;
    @Getter
    private final Map<String, Object> arguments;
    @Getter
    private final RoutingContext routingContext;
    @Getter
    private final SessionManager.McpSession session;
    @Getter
    @Setter
    private String lastEventId;
    @Getter
    @Setter
    private Object executedResult;


    public ToolExecutionContext(String requestId, ToolExecutor tool, Map<String, Object> arguments,
                                RoutingContext routingContext, SessionManager.McpSession session) {
        this.requestId = requestId;
        this.tool = tool;
        this.arguments = arguments;
        this.routingContext = routingContext;
        this.session = session;
    }

    public ToolExecutionContext(String requestId, ToolExecutor tool, Map<String, Object> arguments) {
        this.requestId = requestId;
        this.tool = tool;
        this.arguments = arguments;
        this.routingContext = null;
        this.session = null;
    }

    public boolean isStructuredOutput(){
        return getTool().getOutputSchema()!=null
                && getTool().getOutputSchema().getProperties()!=null
                && !getTool().getOutputSchema().getProperties().isEmpty();
    }
}
package io.vertx.webmvc.mcp;

import java.util.Map;
import java.util.function.Function;
import static io.vertx.webmvc.mcp.McpSchema.*;

public class ToolExecutorImpl implements ToolExecutor {
    private final String name;
    private final String description;
    private final JSONSchema parameters;
    private final JSONSchema outputSchema;
    private final boolean streamingSupported;
    private final Function<Map<String, Object>, Object> executor;

    public ToolExecutorImpl(String name, String description,
                            JSONSchema parameters,JSONSchema outputSchema,
                            boolean streamingSupported, Function<Map<String, Object>, Object> executor) {
        this.name = name;
        this.description = description;
        this.parameters = parameters;
        this.outputSchema = outputSchema;
        this.streamingSupported = streamingSupported;
        this.executor = executor;
    }

    @Override
    public String getName() { return name; }

    @Override
    public String getDescription() { return description; }

    @Override
    public JSONSchema getParameters() { return parameters; }

    @Override
    public JSONSchema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public boolean isStreamingSupported() { return streamingSupported; }

    @Override
    public Object execute(ToolExecutionContext exeCtx) {
        Object result = executor.apply(exeCtx.getArguments());
        exeCtx.setExecutedResult(result);
        return result;
    }
}
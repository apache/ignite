package io.vertx.webmvc.mcp;

import java.util.Map;
import java.util.function.Function;

public class ToolExecutorImpl implements ToolExecutor {
    private final String name;
    private final String description;
    private final Map<String, Object> parameters;
    private final Map<String, Object> outputSchema;
    private final boolean streamingSupported;
    private final Function<Map<String, Object>, Object> executor;

    public ToolExecutorImpl(String name, String description,
                            Map<String, Object> parameters,Map<String, Object> outputSchema,
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
    public Map<String, Object> getParameters() { return parameters; }

    @Override
    public Map<String, Object> getOutputSchema() {
        return outputSchema;
    }

    @Override
    public boolean isStreamingSupported() { return streamingSupported; }

    @Override
    public Object execute(Map<String, Object> arguments) {
        return executor.apply(arguments);
    }
}
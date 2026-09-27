package io.vertx.webmvc.mcp;

import java.util.*;
import java.util.function.Function;
import static io.vertx.webmvc.mcp.McpSchema.*;
/**
 * 工具执行器接口和构建器
 */
public interface ToolExecutor {

    String getName();
    String getDescription();
    JSONSchema getParameters();

    JSONSchema getOutputSchema();

    boolean isStreamingSupported();
    Object execute(ToolExecutionContext exeCtx);

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private String name;
        private String description;
        private JSONSchema parameters = new JSONSchema();
        private JSONSchema outputSchema = null;
        private final Map<String, JSONSchema> outputFields = new HashMap<>();
        private final Map<String, JSONSchema> properties = new HashMap<>();
        private final List<String> required = new ArrayList<>();
        private boolean streamingSupported = false;
        private Function<Map<String, Object>, Object> executor;

        private Function<Map<String, Object>, Iterator<?>> streamExecutor;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder toolInfo(McpToolInfo toolInfo) {
            this.name = toolInfo.getName();
            this.description = toolInfo.getDescription();
            this.parameters = toolInfo.getInputSchema();
            this.outputSchema = toolInfo.getOutputSchema();
            return this;
        }

        public Builder parameter(String name, String type, String description, boolean required) {
            JSONSchema param = new JSONSchema();
            param.setType(type);
            param.setDescription(description);
            properties.put(name, param);

            if (required) {
                this.required.add(name);
            }
            return this;
        }

        public Builder parameter(String name, JSONSchema param,boolean required) {
            properties.put(name, param);
            if (required) {
                this.required.add(name);
            }
            return this;
        }

        public Builder outputField(String name, String type, String description) {
            JSONSchema param = new JSONSchema();
            param.setType(type);
            param.setDescription(description);
            outputFields.put(name, param);
            return this;
        }


        public Builder streamingSupported(boolean supported) {
            this.streamingSupported = supported;
            return this;
        }

        public Builder executor(Function<Map<String, Object>, Object> executor) {
            this.executor = executor;
            return this;
        }

        public Builder streamExecutor(Function<Map<String, Object>, Iterator<?>> executor) {
            this.streamExecutor = executor;
            return this;
        }

        public ToolExecutor build() {
            parameters.setType("object");
            parameters.setProperties(properties);
            if (!required.isEmpty()) {
                parameters.setRequired(required);
            }

            if(!outputFields.isEmpty()){
                outputSchema = new JSONSchema();
                outputSchema.setType("object");
                outputSchema.setProperties(outputFields);
            }

            if(streamExecutor!=null){
                return new StreamingToolExecutorImpl(name, description, parameters, outputSchema, streamExecutor);
            }

            return new ToolExecutorImpl(name, description, parameters, outputSchema,streamingSupported, executor);
        }
    }

}








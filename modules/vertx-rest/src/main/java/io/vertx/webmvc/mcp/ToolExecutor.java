package io.vertx.webmvc.mcp;

import io.vertx.ext.web.RoutingContext;
import lombok.Getter;

import java.util.*;
import java.util.function.Function;

/**
 * 工具执行器接口和构建器
 */
public interface ToolExecutor {

    String getName();
    String getDescription();
    Map<String, Object> getParameters();

    Map<String, Object> getOutputSchema();

    boolean isStreamingSupported();
    Object execute(Map<String, Object> arguments);

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private String name;
        private String description;
        private final Map<String, Object> parameters = new HashMap<>();
        private Map<String, Object> outputSchema = null;
        private final Map<String, Map<String, Object>> outputFields = new HashMap<>();
        private final Map<String, Map<String, Object>> properties = new HashMap<>();
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

        public Builder parameter(String name, String type, String description, boolean required) {
            Map<String, Object> param = new HashMap<>();
            param.put("type", type);
            param.put("description", description);
            properties.put(name, param);

            if (required) {
                this.required.add(name);
            }
            return this;
        }

        public Builder outputField(String name, String type, String description, boolean required) {
            Map<String, Object> param = new HashMap<>();
            param.put("type", type);
            param.put("description", description);
            outputFields.put(name, param);

            if (required) {
                this.required.add(name);
            }
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
            parameters.put("type", "object");
            parameters.put("properties", properties);
            if (!required.isEmpty()) {
                parameters.put("required", required);
            }

            if(!outputFields.isEmpty()){
                outputSchema = new HashMap<>();
                outputSchema.put("type", "object");
                outputSchema.put("properties", outputFields);
            }

            if(streamExecutor!=null){
                return new StreamingToolExecutorImpl(name, description, parameters, outputSchema, streamExecutor);
            }

            return new ToolExecutorImpl(name, description, parameters, outputSchema,streamingSupported, executor);
        }
    }

}








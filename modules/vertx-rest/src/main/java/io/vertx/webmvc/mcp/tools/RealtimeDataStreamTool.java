
package io.vertx.webmvc.mcp.tools;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.mcp.StreamCallback;
import io.vertx.webmvc.mcp.StreamingToolExecutorImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实时数据流工具 - 从 EventBus 订阅实时数据
 */
public class RealtimeDataStreamTool extends StreamingToolExecutorImpl {

    private final Map<String, MessageConsumer<JsonObject>> activeSubscriptions = new HashMap<>();
    private Vertx vertx;

    public RealtimeDataStreamTool(Vertx vertx) {
        super("realtime_stream", "Subscribe to real-time data stream",
                buildParameters(),buildOutputSchema(),null);
        this.vertx = vertx;
    }
    protected static Map<String, Object> buildParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("type", "object");

        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> channelParam = new HashMap<>();
        channelParam.put("type", "string");
        channelParam.put("description", "EventBus channel to subscribe");
        properties.put("channel", channelParam);

        Map<String, Object> durationParam = new HashMap<>();
        durationParam.put("type", "integer");
        durationParam.put("description", "Subscription duration in seconds (0 = unlimited)");
        durationParam.put("default", 60);
        properties.put("duration", durationParam);

        Map<String, Object> filterParam = new HashMap<>();
        filterParam.put("type", "object");
        filterParam.put("description", "Message filter criteria");
        properties.put("filter", filterParam);

        parameters.put("properties", properties);
        parameters.put("required", List.of("channel"));

        return parameters;
    }

    protected static Map<String, Object> buildOutputSchema() {
        return null;
    }
    @Override
    protected void processStream(Map<String, Object> arguments,
                                 StreamEmitter emitter,
                                 StreamCallback callback) {
        String channel = (String) arguments.get("channel");
        int duration = (Integer) arguments.getOrDefault("duration",60);

        Map<String, Object> filter = (Map<String, Object>) arguments.get("filter");

        String taskId = emitter.getTaskId();

        // 订阅 EventBus
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(channel);
        consumer.handler(message -> {
            JsonObject body = message.body();

            // 应用过滤器
            if (filter != null && !matchesFilter(body, filter)) {
                return;
            }

            // 发送数据块
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "message");
            chunk.put("channel", channel);
            chunk.put("body", body.getMap());
            chunk.put("timestamp", System.currentTimeMillis());

            emitter.emitChunk(chunk);
        });

        activeSubscriptions.put(taskId, consumer);

        // 发送订阅确认
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("status", "subscribed");
        metadata.put("channel", channel);
        metadata.put("duration", duration);
        callback.onStart(metadata);

        // 设置超时
        if (duration > 0) {
            vertx.setTimer(duration * 1000, timer -> {
                unsubscribe(taskId);

                Map<String, Object> finalResult = new HashMap<>();
                finalResult.put("status", "completed");
                finalResult.put("duration", duration);
                finalResult.put("message", "Subscription ended");
                callback.onComplete(finalResult);
            });
        }
    }

    private boolean matchesFilter(JsonObject message, Map<String, Object> filter) {
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            Object value = message.getValue(entry.getKey());
            if (!entry.getValue().equals(value)) {
                return false;
            }
        }
        return true;
    }

    private void unsubscribe(String taskId) {
        MessageConsumer<JsonObject> consumer = activeSubscriptions.remove(taskId);
        if (consumer != null) {
            consumer.unregister();
        }
    }
}
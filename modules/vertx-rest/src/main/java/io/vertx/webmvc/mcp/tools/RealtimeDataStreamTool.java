
package io.vertx.webmvc.mcp.tools;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.mcp.StreamCallback;
import io.vertx.webmvc.mcp.StreamingToolExecutorImpl;
import io.vertx.webmvc.mcp.ToolExecutionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static io.vertx.webmvc.mcp.McpSchema.*;

/**
 * 实时数据流工具 - 从 EventBus 订阅实时数据
 */
public class RealtimeDataStreamTool extends StreamingToolExecutorImpl {

    private final Map<String, MessageConsumer<JsonObject>> activeSubscriptions = new HashMap<>();
    private Vertx vertx;

    public RealtimeDataStreamTool(Vertx vertx) {
        super("realtime_stream", "Subscribe to real-time data stream",
                buildParameters());
        this.vertx = vertx;
    }
    protected static JSONSchema buildParameters() {
        // 根节点：整体是 object 类型
        JSONSchema root = new JSONSchema();
        root.setType("object");

        // 构建 properties
        Map<String, JSONSchema> properties = new HashMap<>();

        // 1. channel 参数
        JSONSchema channelParam = new JSONSchema();
        channelParam.setType("string");
        channelParam.setDescription("EventBus channel to subscribe");
        properties.put("channel", channelParam);

        // 2. duration 参数
        JSONSchema durationParam = new JSONSchema();
        durationParam.setType("integer");
        durationParam.setDescription("Subscription duration in seconds (0 = unlimited)");
        durationParam.setDefaultValue(60);
        properties.put("duration", durationParam);

        // 3. filter 参数（嵌套 object）
        JSONSchema filterParam = new JSONSchema();
        filterParam.setType("object");
        filterParam.setDescription("Message filter criteria");
        properties.put("filter", filterParam);

        root.setProperties(properties);
        // 必选参数
        root.setRequired(List.of("channel"));

        return root;
    }

    protected static Map<String, Object> buildOutputSchema() {
        return null;
    }

    @Override
    protected void processStream(ToolExecutionContext exeCtx,
                                 StreamEmitter emitter,
                                 StreamCallback callback) {
        Map<String,Object> arguments = exeCtx.getArguments();
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
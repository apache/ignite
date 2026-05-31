package io.vertx.webmvc.mcp;

import java.util.Map;

/**
 * 流式工具执行器接口
 */
public interface StreamingToolExecutor extends ToolExecutor {
    void executeStreaming(Map<String, Object> arguments, StreamCallback callback);

    @Override
    default boolean isStreamingSupported() {
        return true;
    }
}
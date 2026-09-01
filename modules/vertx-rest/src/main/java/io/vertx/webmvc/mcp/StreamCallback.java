package io.vertx.webmvc.mcp;

import java.util.Map;

/**
 * 流式回调接口
 */
public interface StreamCallback {
    void onStart(Map<String, Object> metadata);
    void onChunk(Map<String, Object> chunk, int sequence);
    void onComplete(Map<String, Object> finalResult);
    void onError(McpSchema.McpError error);

    default void onProgress(Map<String, Object> progress){

    }
}

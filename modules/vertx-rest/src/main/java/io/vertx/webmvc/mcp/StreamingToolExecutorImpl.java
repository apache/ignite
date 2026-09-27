package io.vertx.webmvc.mcp;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static io.vertx.webmvc.mcp.McpSchema.*;

/**
 * 抽象流式工具执行器基类
 * 提供通用的流式处理框架
 */
public class StreamingToolExecutorImpl implements StreamingToolExecutor {
    protected final String name;
    protected final String description;
    protected final JSONSchema parameters;
    protected final JSONSchema outputSchema;
    private final Function<Map<String, Object>, Iterator<?>> executor;

    public StreamingToolExecutorImpl(String name, String description, JSONSchema parameters) {
        this.name = name;
        this.description = description;
        this.parameters = parameters;
        this.outputSchema = null;
        this.executor = null;
    }

    public StreamingToolExecutorImpl(String name, String description,
                                     JSONSchema parameters, JSONSchema outputSchema,
                                     Function<Map<String, Object>, Iterator<?>> executor) {
        this.name = name;
        this.description = description;
        this.parameters = parameters;
        this.outputSchema = outputSchema;
        this.executor = executor;
    }
    /**
     * 执行流式处理逻辑
     */
    protected void processStream(ToolExecutionContext exeCtx,
                                 StreamEmitter emitter,
                                 StreamCallback callback){
        int i = 0;
        long startTime = System.currentTimeMillis();
        Iterator<?> it = executor.apply(exeCtx.getArguments());
        exeCtx.setExecutedResult(it);
        while(it.hasNext()){
            Object next = it.next();
            if(next instanceof Map){
                Map<String, Object> json = (Map<String, Object>)next;
                emitter.emitJsonChunk(json);
            }
            else if(next instanceof String){
                emitter.emitTextChunk(next.toString());
            }
            else if(next instanceof byte[]){
                emitter.emitDataChunk((byte[])next);
            }
            exeCtx.setExecutedResult(next);
        }

        Map<String, Object> completedResult = new HashMap<>();
        completedResult.put("status", "completed");
        completedResult.put("duration", System.currentTimeMillis() - startTime);
        completedResult.put("message", "process completed");
        callback.onComplete(completedResult);
    }

    @Override
    public void executeStreaming(ToolExecutionContext exeCtx, StreamCallback callback) {
        String taskId = generateTaskId();

        // 发送开始元数据
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("taskId", taskId);
        metadata.put("toolName", name);
        metadata.put("startTime", System.currentTimeMillis());
        callback.onStart(metadata);

        // 创建流式发射器
        StreamEmitter emitter = new StreamEmitter(callback, taskId);

        // 执行处理
        try {
            processStream(exeCtx, emitter, callback);
        } catch (Exception e) {
            callback.onError(new McpSchema.McpError(500,e.getMessage(),null));
        }
    }

    protected String generateTaskId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public JSONSchema getParameters() {
        return parameters;
    }

    @Override
    public JSONSchema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public Object execute(ToolExecutionContext exeCtx) {
        // 非流式执行的兼容实现
        StreamResultHandler handler = new StreamResultHandler();
        executeStreaming(exeCtx, handler);
        Object result = handler.getPromise().future().result();
        return result;
    }

    /**
     * 流式发射器 - 辅助发送数据块
     */
    protected static class StreamEmitter {
        private final StreamCallback callback;
        private final String taskId;
        private final AtomicInteger sequence = new AtomicInteger(0);
        private long lastChunkTime;

        public StreamEmitter(StreamCallback callback, String taskId) {
            this.callback = callback;
            this.taskId = taskId;
            this.lastChunkTime = System.currentTimeMillis();
        }

        /**
         * 发送数据块
         */
        public void emitChunk(Map<String, Object> chunk) {
            int seq = sequence.incrementAndGet();
            chunk.put("taskId", taskId);
            chunk.put("timestamp", System.currentTimeMillis());
            callback.onChunk(chunk, seq);
            lastChunkTime = System.currentTimeMillis();
        }

        /**
         * 发送数据块（带内容类型）
         */
        public void emitTextChunk(String text) {
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "text");
            chunk.put("text", text);
            emitChunk(chunk);
        }

        /**
         * 发送数据块（带内容类型）
         */
        public void emitDataChunk(byte[] data) {
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "data");
            chunk.put("data", data);
            chunk.put("size", data.length);
            emitChunk(chunk);
        }

        public void emitImageChunk(byte[] data,String mimeType) {
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "image");
            chunk.put("data", data);
            chunk.put("size", data.length);
            chunk.put("mimeType",mimeType);
            emitChunk(chunk);
        }

        /**
         * 发送 JSON 数据块
         */
        public void emitJsonChunk(Map<String, Object> data) {
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "structured");
            chunk.put("format", "json");
            chunk.put("content", data);
            emitChunk(chunk);
        }

        public void emitProgress(long current,long total,String message) {
            Map<String, Object> chunk = new HashMap<>();
            chunk.put("type", "progress");
            chunk.put("current", current);
            chunk.put("total", total);
            chunk.put("message", message);
            callback.onProgress(chunk);
        }

        public String getTaskId() {
            return taskId;
        }

        public int getCurrentSequence() {
            return sequence.get();
        }
    }


    /**
     * 流式结果处理器（支持 Promise）
     */
    static class StreamResultHandler implements StreamCallback {
        private final Promise<Map<String, Object>> promise = Promise.promise();
        private final java.util.List<Map<String, Object>> chunks = new java.util.ArrayList<>();
        private Map<String, Object> finalResult;
        private Throwable error;
        private boolean completed = false;

        @Override
        public void onStart(Map<String, Object> metadata) {

        }

        @Override
        public void onChunk(Map<String, Object> chunk, int sequence) {
            chunks.add(chunk);
        }

        @Override
        public void onComplete(Map<String, Object> finalResult) {
            this.finalResult = finalResult;
            this.completed = true;
            promise.complete(finalResult);
        }

        @Override
        public void onError(McpSchema.McpError error) {
            this.error = error;
            promise.fail(error);
        }

        public Promise<Map<String, Object>> getPromise() {
            return promise;
        }

        public java.util.List<Map<String, Object>> getChunks() {
            return java.util.Collections.unmodifiableList(chunks);
        }

        public Map<String, Object> getFinalResult() {
            return finalResult;
        }

        public Throwable getError() {
            return error;
        }

        public boolean isCompleted() {
            return completed;
        }
    }
}

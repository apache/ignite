package io.vertx.webmvc.mcp.tools;

import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.webmvc.mcp.McpSchema;
import io.vertx.webmvc.mcp.StreamingToolExecutorImpl;
import io.vertx.webmvc.mcp.StreamCallback;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 流式文件处理工具
 * 逐行读取大文件并实时返回内容
 */
public class StreamFileProcessorTool extends StreamingToolExecutorImpl {
    private Vertx vertx;
    private final FileSystem fileSystem;

    public StreamFileProcessorTool(Vertx vertx) {
        super("stream_file", "Stream large file content line by line",
                buildParameters(),buildOutputSchema(),null);
        this.vertx = vertx;
        this.fileSystem = vertx.fileSystem();
    }

    protected static Map<String, Object> buildParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("type", "object");

        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> filePathParam = new HashMap<>();
        filePathParam.put("type", "string");
        filePathParam.put("description", "Path to the file to process");
        properties.put("filePath", filePathParam);

        Map<String, Object> batchSizeParam = new HashMap<>();
        batchSizeParam.put("type", "integer");
        batchSizeParam.put("description", "Number of lines per batch");
        batchSizeParam.put("default", 100);
        properties.put("batchSize", batchSizeParam);

        Map<String, Object> encodingParam = new HashMap<>();
        encodingParam.put("type", "string");
        encodingParam.put("description", "File encoding");
        encodingParam.put("default", "UTF-8");
        properties.put("encoding", encodingParam);

        parameters.put("properties", properties);
        parameters.put("required", java.util.List.of("filePath"));

        return parameters;
    }

    protected static Map<String, Object> buildOutputSchema() {
        return null;
    }

    @Override
    protected void processStream(Map<String, Object> arguments,
                                 StreamEmitter emitter,
                                 StreamCallback callback) {
        String filePath = (String) arguments.get("filePath");
        int batchSize = arguments.containsKey("batchSize") ?
                ((Number) arguments.get("batchSize")).intValue() : 100;
        String encoding = (String) arguments.getOrDefault("encoding", "UTF-8");

        // 异步处理文件
        vertx.executeBlocking(promise -> {
            try {
                processFile(filePath, batchSize, encoding, emitter, callback);
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }, result -> {
            if (result.failed()) {
                callback.onError(new McpSchema.McpError(500,result.cause().getMessage(),result.result()));
            }
        });
    }

    private void processFile(String filePath, int batchSize, String encoding,
                             StreamEmitter emitter, StreamCallback callback) throws Exception {

        final AtomicInteger totalLines = new AtomicInteger();

        // 先检查文件是否存在
        fileSystem.exists(filePath, ar -> {
            if (ar.failed() || !ar.result()) {
                callback.onError(new McpSchema.McpError(404,"File not found: " + filePath, ar.result()));
                return;
            }

            // 获取文件大小
            fileSystem.props(filePath, propsAr -> {
                if (propsAr.succeeded()) {
                    long fileSize = propsAr.result().size();
                    // 预测totalLines
                    totalLines.set((int)fileSize/80/8);
                    Map<String, Object> metadata = new HashMap<>();
                    metadata.put("fileSize", fileSize);
                    metadata.put("filePath", filePath);
                    metadata.put("batchSize", batchSize);
                    callback.onStart(metadata);
                }
            });
        });

        // 使用传统的 BufferedReader 逐行读取
        try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(
                new java.io.FileInputStream(filePath), encoding))) {

            String line;
            java.util.List<String> batch = new java.util.ArrayList<>();

            int batchCount = 0;

            while ((line = reader.readLine()) != null) {
                batch.add(line);

                if(batchCount*batchSize>totalLines.get())
                    totalLines.addAndGet(batchSize);

                // 达到批次大小，发送数据块
                if (batch.size() >= batchSize) {
                    batchCount++;
                    sendBatch(batch, batchCount, totalLines.get(), emitter, callback);
                    batch.clear();

                    // 模拟延迟，避免过快发送
                    Thread.sleep(100);
                }
            }

            // 发送最后一批
            if (!batch.isEmpty()) {
                batchCount++;
                sendBatch(batch, batchCount, totalLines.get(), emitter, callback);
            }

            // 发送完成结果
            Map<String, Object> finalResult = new HashMap<>();
            finalResult.put("status", "success");
            finalResult.put("totalLines", totalLines);
            finalResult.put("totalBatches", batchCount);
            finalResult.put("filePath", filePath);
            callback.onComplete(finalResult);

        } catch (Exception e) {
            callback.onError(new McpSchema.McpError(500,e.getMessage(),null));
        }
    }

    private void sendBatch(java.util.List<String> batch, int batchNum,
                           int totalLines, StreamEmitter emitter,
                           StreamCallback callback) {
        Map<String, Object> chunk = new HashMap<>();
        chunk.put("type", "batch");
        chunk.put("batchNumber", batchNum);
        chunk.put("lineCount", batch.size());
        chunk.put("lines", batch);
        chunk.put("progress", calculateProgress(batchNum, totalLines));

        emitter.emitChunk(chunk);

        // 发送进度更新
        int progress = (int) ((double) batchNum / (totalLines / 100.0 + 1));
        emitter.emitProgress(progress, 100, String.format("Processed batch %d, total lines: %d", batchNum, totalLines));
    }

    private int calculateProgress(int batchNum, int totalLines) {
        return Math.min(100, (batchNum * 100) / (totalLines / 100 + 1));
    }
}
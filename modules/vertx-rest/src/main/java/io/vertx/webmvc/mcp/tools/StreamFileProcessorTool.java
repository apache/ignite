package io.vertx.webmvc.mcp.tools;

import io.vertx.core.Vertx;
import io.vertx.webmvc.mcp.McpSchema;
import io.vertx.webmvc.mcp.StreamingToolExecutorImpl;
import io.vertx.webmvc.mcp.StreamCallback;
import io.vertx.webmvc.mcp.ToolExecutionContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static io.vertx.webmvc.mcp.McpSchema.*;

/**
 * 流式文件处理工具
 * 逐行读取大文件并实时返回内容
 */
public class StreamFileProcessorTool extends StreamingToolExecutorImpl {
    private Vertx vertx;
    String igniteInstanceName;

    public StreamFileProcessorTool(Vertx vertx,String igniteInstanceName) {
        super("stream_file", "Stream large file content line by line",
                buildParameters());
        this.vertx = vertx;
        this.igniteInstanceName = igniteInstanceName;

    }

    protected static JSONSchema buildParameters() {
        JSONSchema root = new JSONSchema();
        root.setType("object");

        Map<String, JSONSchema> properties = new HashMap<>();

        // filePath 参数
        JSONSchema filePathParam = new JSONSchema();
        filePathParam.setType("string");
        filePathParam.setDescription("Path to the file to process");
        properties.put("filePath", filePathParam);

        // batchSize 参数
        JSONSchema batchSizeParam = new JSONSchema();
        batchSizeParam.setType("integer");
        batchSizeParam.setDescription("Number of lines per batch");
        batchSizeParam.setDefaultValue(100);
        properties.put("batchSize", batchSizeParam);

        // encoding 参数
        JSONSchema encodingParam = new JSONSchema();
        encodingParam.setType("string");
        encodingParam.setDescription("File encoding");
        encodingParam.setDefaultValue("UTF-8");
        properties.put("encoding", encodingParam);

        root.setProperties(properties);
        root.setRequired(List.of("filePath"));

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
        String filePath = (String) arguments.get("filePath");
        int batchSize = arguments.containsKey("batchSize") ?
                ((Number) arguments.get("batchSize")).intValue() : 100;
        String encoding = (String) arguments.getOrDefault("encoding", "UTF-8");
        Ignite ignite = Ignition.ignite(igniteInstanceName);
        String[] parts = filePath.split("/",2);
        IgniteFileSystem fileSystem = ignite.fileSystem(parts[0]);
        if(fileSystem==null || parts.length<2){
            callback.onError(new McpSchema.McpError(404,"File System not found.",null));
        }
        // 异步处理文件
        vertx.executeBlocking(promise -> {
            try {
                processFile(fileSystem,"/"+parts[1], batchSize, encoding, emitter, callback);
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

    private void processFile(IgniteFileSystem fileSystem, String filePath, int batchSize, String encoding,
                             StreamEmitter emitter, StreamCallback callback) throws Exception {

        final AtomicInteger totalLines = new AtomicInteger();
        long fileSize = 0;
        long readed = 0;
        // 先检查文件是否存在
        IgfsPath path = new IgfsPath(filePath);
        if(!fileSystem.exists(path)){
            callback.onError(new McpSchema.McpError(404,"File not found: " + filePath, filePath));
            return;
        }
        else{
            IgfsFile file = fileSystem.info(path);
            // 获取文件大小
            fileSize = file.length();
            Map<String, Object> metadata = new HashMap<>();
            metadata.putAll(file.properties());
            metadata.put("fileSize", fileSize);
            metadata.put("filePath", filePath);
            metadata.put("batchSize", batchSize);
            callback.onStart(metadata);
        }

        // 使用传统的 BufferedReader 逐行读取
        try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(
                fileSystem.open(path), encoding))) {

            String line;
            java.util.List<String> batch = new java.util.ArrayList<>();

            int batchCount = 0;

            while ((line = reader.readLine()) != null) {
                batch.add(line);

                readed+=line.length()+1;

                // 达到批次大小，发送数据块
                if (batch.size() >= batchSize) {
                    batchCount++;
                    sendBatch(batch, batchCount, emitter);
                    batch.clear();

                    // 发送进度更新
                    if(batchCount%5==0)
                        emitter.emitProgress(readed, fileSize, String.format("Readed: %d, totalSize : %d", readed, fileSize));

                    // 模拟延迟，避免过快发送
                    Thread.sleep(10);
                }
            }

            // 发送最后一批
            if (!batch.isEmpty()) {
                batchCount++;
                sendBatch(batch, batchCount, emitter);
            }

            // 发送完成结果
            Map<String, Object> finalResult = new HashMap<>();
            finalResult.put("status", "success");
            finalResult.put("totalLines", totalLines);
            finalResult.put("totalBatches", batchCount);

            callback.onComplete(finalResult);

        } catch (Exception e) {
            callback.onError(new McpSchema.McpError(500,e.getMessage(),null));
        }
    }

    private void sendBatch(java.util.List<String> batch, int batchNum,
                          StreamEmitter emitter) {
        Map<String, Object> chunk = new HashMap<>();
        chunk.put("type", "data");
        chunk.put("batchNum", batchNum);
        chunk.put("lineCount", batch.size());
        chunk.put("data", batch);
        emitter.emitChunk(chunk);
    }

}
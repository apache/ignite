package io.vertx.webmvc.mcp.tools;

import io.vertx.core.Vertx;
import io.vertx.webmvc.mcp.StreamCallback;
import io.vertx.webmvc.mcp.StreamingToolExecutorImpl;
import io.vertx.webmvc.mcp.ToolExecutionContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.rest.igfs.util.FileUtil;

import java.io.BufferedReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vertx.webmvc.mcp.McpSchema.JSONSchema;
import static io.vertx.webmvc.mcp.McpSchema.McpError;

/**
 * 流式文件处理工具
 * 逐行读取文件内容搜索关键词，实时返回搜索到的文件和行数
 */
public class StreamFileSearchProcessorTool extends StreamingToolExecutorImpl {
    private Vertx vertx;
    String igniteInstanceName;

    public StreamFileSearchProcessorTool(Vertx vertx, String igniteInstanceName) {
        super("stream_search_file", "Stream search file content line by line",
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
        filePathParam.setDescription("Root path to the file to process");
        properties.put("filePath", filePathParam);

        // keyword 参数
        JSONSchema keyword = new JSONSchema();
        keyword.setType("string");
        keyword.setDescription("Search keyword，support regex");
        properties.put("keyword", keyword);

        // exts 参数
        JSONSchema exts = new JSONSchema();
        exts.setType("string");
        exts.setDescription("File exts. Sep by ,");
        exts.setDefaultValue("");
        properties.put("exts", exts);

        // windowSize 参数
        JSONSchema batchSizeParam = new JSONSchema();
        batchSizeParam.setType("integer");
        batchSizeParam.setDescription("Number of chars can see per line which hava keyword");
        batchSizeParam.setDefaultValue(80);
        properties.put("windowSize", batchSizeParam);

        // encoding 参数
        JSONSchema encodingParam = new JSONSchema();
        encodingParam.setType("string");
        encodingParam.setDescription("File encoding");
        encodingParam.setDefaultValue("UTF-8");
        properties.put("encoding", encodingParam);

        root.setProperties(properties);
        root.setRequired(List.of("filePath","keyword"));

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
        int windowSize = arguments.containsKey("windowSize") ?
                ((Number) arguments.get("windowSize")).intValue() : 80;
        String keyword = (String) arguments.getOrDefault("keyword", "");
        String exts = (String) arguments.getOrDefault("exts", "");
        String encoding = (String) arguments.getOrDefault("encoding", "UTF-8");
        Ignite ignite = Ignition.ignite(igniteInstanceName);
        String[] parts = filePath.split("/",2);
        String[] fileExts = exts.split(",");
        IgniteFileSystem fileSystem = ignite.fileSystem(parts[0]);
        if(fileSystem==null){
            callback.onError(new McpError(404,"File System not found.",null));
            return;
        }
        if(parts.length<2){
            callback.onError(new McpError(404,"File Path must be dir.",null));
            return;
        }
        // 异步处理文件
        vertx.executeBlocking(promise -> {
            try {
                processFile(fileSystem,"/"+parts[1], Set.of(fileExts), keyword, windowSize, encoding, emitter, callback);
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }, result -> {
            if (result.failed()) {
                callback.onError(new McpError(500,result.cause().getMessage(),result.result()));
            }
        });
    }

    private void processFile(IgniteFileSystem fileSystem, String filePath, Set<String> fileExts, String keyword,
                             int windowSize,String encoding,
                             StreamEmitter emitter, StreamCallback callback) throws Exception {

        int totalFile = 0;
        long fileSize = 0;
        long readed = 0;
        // 先检查文件是否存在
        IgfsPath path = new IgfsPath(filePath);
        if(!fileSystem.exists(path)){
            callback.onError(new McpError(404,"File not found: " + filePath, filePath));
            return;
        }
        else{
            IgfsFile file = fileSystem.info(path);
            // 获取文件大小

            Map<String, Object> metadata = new HashMap<>();
            metadata.putAll(file.properties());
            metadata.put("filePath", filePath);
            callback.onStart(metadata);

            try {
                // 使用传统的 BufferedReader 逐行读取
                totalFile = processOneFile(fileSystem,file,fileExts,keyword,windowSize,encoding,emitter,callback);

                // 发送完成结果
                Map<String, Object> finalResult = new HashMap<>();
                finalResult.put("status", "success");
                finalResult.put("totalFiles", totalFile);

                callback.onComplete(finalResult);

            } catch (Exception e) {
                callback.onError(new McpError(500,e.getMessage(),null));
            }
        }
    }

    private int processOneFile(IgniteFileSystem fileSystem, IgfsFile file, Set<String> fileExts, String keyword,
                             int windowSize,String encoding,
                             StreamEmitter emitter, StreamCallback callback) throws Exception {


        int lineNo = 0;
        long readed = 0;
        int totalFile = 0;
        if(file.isDirectory()){
            for(var subFile: fileSystem.listFiles(file.path())){
                if(!fileExts.isEmpty()){
                    String ext = FileUtil.getExtension(subFile.path().name());
                    if(!fileExts.contains(ext)){
                        continue;
                    }
                }
                totalFile+=processOneFile(fileSystem,subFile,fileExts,keyword,windowSize,encoding,emitter,callback);
            }
            return totalFile;
        }

        IgfsPath path = file.path();
        if(!fileSystem.exists(file.path())){
            callback.onError(new McpError(404,"File not found: " + file, null));
            return totalFile;
        }

        // 使用传统的 BufferedReader 逐行读取
        try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(
                fileSystem.open(path), encoding))) {

            String line;

            while ((line = reader.readLine()) != null) {
                lineNo++;
                readed+=line.length()+1;
                int pos = line.indexOf(keyword);
                if(pos<0)
                    continue;

                String data = line.substring(Math.max(pos-windowSize,0),Math.min(pos+windowSize,line.length()));
                sendResult(path.toString(),lineNo,pos,data,emitter);
            }

        } catch (Exception e) {
            callback.onError(new McpError(500,e.getMessage(),null));
        }
        return totalFile;
    }

    private void sendResult(String path, int lineNum, int pos, String data, StreamEmitter emitter) {
        Map<String, Object> chunk = new HashMap<>();
        chunk.put("type", "data");
        chunk.put("pos", pos);
        chunk.put("path", path);
        chunk.put("lineNum", lineNum);
        chunk.put("data", data);
        emitter.emitChunk(chunk);
    }

}
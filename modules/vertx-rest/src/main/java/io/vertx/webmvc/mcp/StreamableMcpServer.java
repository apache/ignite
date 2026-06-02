package io.vertx.webmvc.mcp;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.webmvc.mcp.tools.RealtimeDataStreamTool;
import io.vertx.webmvc.mcp.tools.StreamFileProcessorTool;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.springframework.context.ApplicationContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.vertx.core.json.jackson.DatabindCodec;

import static io.vertx.webmvc.mcp.McpSchema.*;

/**
 * 可扩展的流式MCP服务器实现
 * 支持工具注册、流式响应、会话管理和扩展机制
 */
public class StreamableMcpServer extends AbstractVerticle {

    private static final Logger LOGGER = Logger.getLogger(StreamableMcpServer.class.getName());

    // 工具注册表
    private final Map<String, ToolExecutor> toolRegistry = new ConcurrentHashMap<>();

    // 工具拦截器
    private final List<ToolInterceptor> toolInterceptors = new ArrayList<>();

    // 会话管理器
    private final SessionManager sessionManager = new SessionManager();

    // Spring上下文
    private ApplicationContext springContext;

    // HTTP服务器配置
    private Supplier<HttpServer> httpServer;

    // 服务器能力配置
    private ServerCapabilities serverCapabilities;

    // 请求日志记录器
    private RequestLogger requestLogger;

    private RealtimeDataStreamTool realtimeDataStreamTool;

    protected String igniteInstanceName;

    public void setIgniteInstanceName(String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;
    }

    public StreamableMcpServer(ApplicationContext applicationContext, Supplier<HttpServer> server) {
        this.springContext = applicationContext;
        this.httpServer = server;
        initializeServer();
    }


    private void initializeServer() {
        // 初始化服务器能力
        this.serverCapabilities = new ServerCapabilities();
        this.serverCapabilities.setProtocolVersion("2024-11-05");
        this.serverCapabilities.setTools(Map.of("listChanged", true));
        this.serverCapabilities.setStreaming(Map.of("supported", true, "maxChunkSize", 4096));

        // 初始化请求日志记录器
        this.requestLogger = new RequestLogger(vertx);

        // 从Spring上下文加载工具
        loadToolsFromSpringContext();
    }



    @Override
    public void start() {
        // 注册内置工具
        registerBuiltinTools();

        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // 如果需要也配置 pretty mapper
        ObjectMapper prettyMapper = DatabindCodec.prettyMapper();
        prettyMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // 创建路由器并配置
        Router router = Router.router(vertx);

        // 配置请求体处理
        router.route().handler(BodyHandler.create());

        // 配置日志记录
        router.route("/").handler(this::logRequest);

        // MCP请求处理端点
        router.post("/").handler(this::handleMcpRequest);

        // 健康检查端点
        router.get("/health").handler(this::handleHealthCheck);

        // 工具列表端点（可选）
        router.get("/tools").handler(this::handleToolsListEndpoint);

        // SSE流式端点
        router.get("/stream").handler(this::handleStreamConnection);

        Router mainRouter = (Router)httpServer.get().requestHandler();
        mainRouter.post("/mcp").handler(this::handleMcpRequest);
        mainRouter.route("/mcp/*").subRouter(router);

        // 404 处理器 - 确保未匹配的路由也有响应
        router.route().last().handler(ctx -> {
            if (!ctx.response().ended()) {
                ctx.response()
                        .setStatusCode(404)
                        .end("{\"error\": \"Not Found\"}");
            }
        });
    }

    private void logRequest(RoutingContext ctx) {
        long startTime = System.currentTimeMillis();
        ctx.put("startTime", startTime);
        ctx.addBodyEndHandler(v -> {
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.info(String.format("%s %s - %d ms - %d",
                    ctx.request().method(),
                    ctx.request().path(),
                    duration,
                    ctx.response().getStatusCode()));
        });
        ctx.next();
    }

    private void handleServerError(Throwable error) {
        LOGGER.log(Level.SEVERE, "Server error: " + error.getMessage(), error);
    }

    private void handleHealthCheck(RoutingContext ctx) {
        JsonObject health = new JsonObject()
                .put("status", "UP")
                .put("timestamp", System.currentTimeMillis())
                .put("toolsCount", toolRegistry.size())
                .put("activeSessions", sessionManager.getActiveSessionCount());
        ctx.response().end(health.encodePrettily());
    }

    private void handleToolsListEndpoint(RoutingContext ctx) {
        List<McpToolInfo> tools = getToolsList();
        JsonObject response = new JsonObject()
                .put("tools", tools)
                .put("count", tools.size());
        ctx.response().end(response.encodePrettily());
    }

    private void handleStreamConnection(RoutingContext ctx) {
        String sessionId = ctx.request().getParam("sessionId");
        if (sessionId == null) {
            sessionId = ctx.request().getHeader("Mcp-Session-Id");
        }
        if (sessionId == null) {
            sessionId = generateSessionId();
        }
        Map<String, String> arguments = parameters(ctx.request());

        ctx.response()
                .putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .putHeader("Connection", "keep-alive")
                .setChunked(true);

        // 创建流式会话
        Map<String, Object> clientInfo = new HashMap<>();
        SessionManager.StreamSession session = sessionManager.createStreamSession(sessionId, clientInfo, ctx);
        // 发送连接成功事件
        sendStreamEvent(ctx, "connected", new JsonObject().put("sessionId", sessionId));

        String xsessionId = sessionId;

        // 执行拦截器链
        ToolExecutionContext executionContext = new ToolExecutionContext(
                xsessionId,
                realtimeDataStreamTool,
                (Map)arguments,
                ctx,
                session
        );

        realtimeDataStreamTool.executeStreaming(executionContext,new StreamCallback() {
            @Override
            public void onStart(Map<String, Object> metadata) {
                // 发送开始事件
                sendStreamEvent(ctx, "metadata", new JsonObject(metadata));
            }

            @Override
            public void onChunk(Map<String, Object> chunk, int sequence) {
                JsonObject data = new JsonObject(chunk)
                        .put("sequence", sequence);
                sendStreamEvent(ctx, "", data);
            }

            @Override
            public void onComplete(Map<String, Object> finalResult) {
                JsonObject data = new JsonObject(finalResult)
                        .put("final", true);
                sendStreamEvent(ctx, "complete", data);
                ctx.response().end();
            }

            @Override
            public void onError(McpError error) {
                JsonObject data = new JsonObject();
                data.put("type", "error");
                data.put("code", error.getCode());
                data.put("error", error.getMessage());
                data.put("data", error.getData());
                sendStreamEvent(ctx, "error", data);
                ctx.response().end();
            }
        });
        // 处理连接关闭
        ctx.response().closeHandler(v -> {
            sessionManager.removeSession(xsessionId);
            LOGGER.info("Stream session closed: " + xsessionId);
        });
    }

    private void sendStreamEvent(RoutingContext ctx, String event, JsonObject data) {
        Buffer buffer = Buffer.buffer();
        if (event != null && !event.isEmpty()) {
            buffer.appendString("event: ").appendString(event).appendString("\n");
        }
        buffer.appendString("data: ").appendString(data.encode()).appendString("\n\n");
        ctx.response().write(buffer);
    }

    private String generateSessionId() {
        return UUID.randomUUID().toString();
    }

    private void handleMcpRequest(RoutingContext ctx) {
        try {
            String body = ctx.getBodyAsString();
            if (body == null || body.trim().isEmpty()) {
                sendError(ctx, null, "Empty request body", -32700);
                return;
            }

            // 记录请求
            requestLogger.logRequest(ctx.request(), body);

            // 解析请求
            McpRequest request = Json.decodeValue(body, McpRequest.class);

            // 验证请求
            if (!validateRequest(request) && !request.getMethod().endsWith("initialized")) {
                sendError(ctx, request.getId(), "Invalid request", -32600);
                return;
            }

            // 处理请求
            switch (request.getMethod()) {
                case "initialize":
                    handleInitialize(ctx, request);
                    break;
                case "notifications/initialized":
                case "initialized":
                    handleInitialized(ctx, request);
                    break;
                case "tools/list":
                    handleToolsList(ctx, request);
                    break;
                case "tools/call":
                    handleToolsCall(ctx, request);
                    break;
                case "ping":
                    handlePing(ctx, request);
                    break;
                case "shutdown":
                    handleShutdown(ctx, request);
                    break;
                default:
                    sendError(ctx, request.getId(), "Method not found: " + request.getMethod(), -32601);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error handling MCP request: " + e.getMessage(), e);
            sendError(ctx, null, "Invalid request format: " + e.getMessage(), -32700);
        }
    }

    private boolean validateRequest(McpRequest request) {
        return request != null &&
                request.getMethod() != null &&
                request.getId() != null &&
                !request.getId().isEmpty();
    }

    private void handleInitialize(RoutingContext ctx, McpRequest request) {
        try {
            // 获取客户端信息
            Map<String, Object> clientInfo = request.getParams() != null ?
                    request.getParams().getClientInfo() : null;

            // 生成会话ID
            String sessionId = generateSessionId();

            // 创建会话
            sessionManager.createSession(sessionId, clientInfo);

            // 构建响应
            Map<String, Object> result = new HashMap<>();
            result.put("protocolVersion", serverCapabilities.getProtocolVersion());
            result.put("capabilities", serverCapabilities.toMap());
            result.put("sessionId", sessionId);
            result.put("serverInfo", Map.of(
                    "name", "Vert.x MCP Server",
                    "version", "1.0.0"
            ));

            sendStandardResponse(ctx, request, result);
        } catch (Exception e) {
            sendError(ctx, request.getId(), "Initialization failed: " + e.getMessage(), -32000);
        }
    }

    private void handleInitialized(RoutingContext ctx, McpRequest request) {
        // 客户端确认初始化完成
        String sessionId = request.getParams() != null ?
                request.getParams().getSessionId() : null;

        if (sessionId != null) {
            sessionManager.confirmSession(sessionId);
        }

        // 返回空响应表示成功
        ctx.response().setStatusCode(204).end();
    }

    private void handlePing(RoutingContext ctx, McpRequest request) {
        // 心跳响应
        Map<String, Object> result = Map.of("status", "pong", "timestamp", System.currentTimeMillis());
        sendStandardResponse(ctx, request, result);
    }

    private void handleShutdown(RoutingContext ctx, McpRequest request) {
        // 优雅关闭
        vertx.executeBlocking(promise -> {
            try {
                // 清理资源
                sessionManager.closeAllSessions();
                // 计划关闭服务器
                vertx.setTimer(1000, timer -> {
                    try {
                        this.stop();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                promise.complete();
            } catch (Exception e) {
                promise.fail(e);
            }
        }, result -> {
            if (result.succeeded()) {
                Map<String, Object> resultMap = Map.of("status", "shutting_down");
                sendStandardResponse(ctx, request, resultMap);
            } else {
                sendError(ctx, request.getId(), "Shutdown failed: " + result.cause().getMessage(), -32000);
            }
        });
    }

    private void handleToolsList(RoutingContext ctx, McpRequest request) {
        List<McpToolInfo> tools = getToolsList();

        Map<String, Object> result = new HashMap<>();
        result.put("tools", tools);

        // 如果支持工具列表变更通知
        if (Boolean.TRUE.equals(serverCapabilities.getTools().get("listChanged"))) {
            result.put("nextCursor", null);
        }

        sendStandardResponse(ctx, request, result);
    }

    private List<McpToolInfo> getToolsList() {
        List<McpToolInfo> tools = new ArrayList<>();
        for (Map.Entry<String, ToolExecutor> entry : toolRegistry.entrySet()) {
            ToolExecutor toolEx = entry.getValue();
            McpToolInfo toolInfo = new McpToolInfo();
            toolInfo.setName(toolEx.getName());
            toolInfo.setDescription(toolEx.getDescription());
            toolInfo.setInputSchema(toolEx.getParameters());
            toolInfo.setOutputSchema(toolEx.getOutputSchema());

            ToolMeta meta = new ToolMeta();
            meta.setIsStreaming(toolEx.isStreamingSupported());
            toolInfo.setMeta(meta);
            tools.add(toolInfo);
        }
        return tools;
    }

    private void handleToolsCall(RoutingContext ctx, McpRequest request) {
        String acceptHeader = ctx.request().getHeader("Accept");
        String toolName = request.getParams().getName();
        Map<String, Object> arguments = request.getParams().getArguments();

        String sessionId = ctx.request().getParam("sessionId");
        if (sessionId == null) {
            sessionId = ctx.request().getHeader("Mcp-Session-Id");
        }

        // 验证工具是否存在
        ToolExecutor tool = toolRegistry.get(toolName);
        if (tool == null) {
            sendError(ctx, request.getId(), "Tool not found: " + toolName, -32601);
            return;
        }

        // 执行拦截器链
        ToolExecutionContext executionContext = new ToolExecutionContext(
                request.getId(),
                tool,
                arguments,
                ctx,
                sessionManager.getSession(sessionId)
        );

        // 执行前置拦截器
        for (ToolInterceptor interceptor : toolInterceptors) {
            if (!interceptor.beforeExecute(executionContext)) {
                sendError(ctx, request.getId(), "Intercepted: " + interceptor.getClass().getSimpleName(), -32000);
                return;
            }
        }

        // 判断是否需要流式响应
        boolean shouldStream = determineStreamingMode(acceptHeader, tool, ctx);

        // 执行工具调用
        try {

            if (shouldStream && tool.isStreamingSupported()) {
                executeStreamingTool(executionContext, request, tool);
            } else {
                executeStandardTool(executionContext, request, tool);
            }

        } catch (Exception e) {
            // 执行错误拦截器
            for (ToolInterceptor interceptor : toolInterceptors) {
                interceptor.onError(executionContext, e);
            }
            sendError(ctx, request.getId(), "Tool execution failed: " + e.getMessage(), -32000);
        }
    }

    private boolean determineStreamingMode(String acceptHeader, ToolExecutor tool, RoutingContext ctx) {
        // 条件1：请求头明确要求SSE
        if ("text/event-stream".equals(acceptHeader)) {
            return true;
        }

        // 条件2：工具声明支持流式输出
        if (tool.isStreamingSupported()) {
            return true;
        }

        // 条件3：恢复中断的会话
        String lastEventId = ctx.request().getHeader("Last-Event-ID");
        if (lastEventId != null) {
            return true;
        }

        // 条件4：请求体指定了流式输出
        String preferStream = ctx.request().getHeader("X-Prefer-Stream");
        if ("true".equals(preferStream)) {
            return true;
        }

        return false;
    }

    private void executeStandardTool(ToolExecutionContext exeCtx, McpRequest request, ToolExecutor tool) {
        long startTime = System.currentTimeMillis();
        RoutingContext ctx = exeCtx.getRoutingContext();

        // 验证参数
        validateToolArguments(tool, exeCtx.getArguments());

        // 执行工具
        Object result = tool.execute(exeCtx);

        // 执行后置拦截器
        for (ToolInterceptor interceptor : toolInterceptors) {
            interceptor.afterExecute(exeCtx, result);
        }
        result = exeCtx.getExecutedResult();

        // 记录执行时间
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.fine(String.format("Tool %s executed in %d ms", tool.getName(), duration));

        // 构建响应
        McpToolCallResult toolResult = new McpToolCallResult();
        boolean isStructureOutput = exeCtx.isStructuredOutput();
        if(!isStructureOutput) {
            if(result instanceof Collection && !(result instanceof List)){
                result = new ArrayList<>((Collection)result);
            }
            if(result instanceof List){
                List list = (List)result;
                if(!list.isEmpty() && list.get(0) instanceof Map){
                    toolResult.setContent(list);
                }
                if(!list.isEmpty()){
                    toolResult.setContent(list.stream().map(item->Map.of("type", "text", "text", item.toString())).toList());
                }
                else{
                    toolResult.setContent(list);
                }
            }
            else if(result instanceof CharSequence)
                toolResult.setContent(Collections.singletonList(Map.of("type", "text", "text", result.toString())));
            else{
                JsonObject json = JsonObject.mapFrom(result);
                toolResult.setContent(Collections.singletonList(Map.of("type", "text", "text", json.toString())));
            }
        }
        else if(result instanceof Map){
            toolResult.setStructuredContent((Map)result);
        }
        else{
            JsonObject json = JsonObject.mapFrom(result);
            toolResult.setStructuredContent(json.getMap());
        }
        toolResult.setExecutionTime(duration);

        sendStandardResponse(ctx, request, toolResult);
    }

    private void executeStreamingTool(ToolExecutionContext exeCtx, McpRequest request,ToolExecutor tool) {
        RoutingContext ctx = exeCtx.getRoutingContext();
        // 设置SSE响应头
        ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .putHeader("Connection", "keep-alive")
                .putHeader("X-Accel-Buffering", "no")  // 禁用nginx缓冲
                .setChunked(true);

        String lastEventId = ctx.request().getHeader("Last-Event-ID");
        exeCtx.setLastEventId(lastEventId);

        // 执行流式工具
        if (tool instanceof StreamingToolExecutor) {
            StreamingToolExecutor streamingTool = (StreamingToolExecutor) tool;
            streamingTool.executeStreaming(exeCtx, new StreamCallback() {
                @Override
                public void onStart(Map<String, Object> metadata) {
                    // 发送开始事件
                    sendStreamEvent(ctx, "metadata", new JsonObject(metadata));
                }

                @Override
                public void onChunk(Map<String, Object> chunk, int sequence) {
                    JsonObject data = new JsonObject(chunk)
                            .put("sequence", sequence);
                    sendStreamEvent(ctx, "", data);
                }

                @Override
                public void onComplete(Map<String, Object> finalResult) {
                    JsonObject data = new JsonObject(finalResult)
                            .put("final", true);
                    sendStreamEvent(ctx, "complete", data);
                    ctx.response().end();
                }

                @Override
                public void onError(McpError error) {
                    JsonObject data = new JsonObject();
                    data.put("type", "error");
                    data.put("code", error.getCode());
                    data.put("error", error.getMessage());
                    data.put("data", error.getData());
                    sendStreamEvent(ctx, "error", data);
                    ctx.response().end();
                }
            });
            // 执行后置拦截器
            for (ToolInterceptor interceptor : toolInterceptors) {
                interceptor.afterExecute(exeCtx, null);
            }

        } else {
            // 普通工具模拟流式输出
            Object result = tool.execute(exeCtx);
            // 执行后置拦截器
            for (ToolInterceptor interceptor : toolInterceptors) {
                interceptor.afterExecute(exeCtx, result);
            }
            result = exeCtx.getExecutedResult();

            if(result instanceof Iterable<?>) {
                sendStreamingResponse(ctx, request, (Iterable)result);
            }
            else{
                JsonObject data = new JsonObject()
                        .put("result", result);
                sendStreamingResponse(ctx, request, data);
            }
        }
    }

    private void validateToolArguments(ToolExecutor tool, Map<String, Object> arguments) {
        JSONSchema parameters = tool.getParameters();
        if (parameters == null || parameters.getProperties()==null) {
            return;
        }

        // 放默认值
        Map<String, JSONSchema> properties = parameters.getProperties();
        properties.forEach((k,v)->{
            if(v.getDefaultValue()!=null && arguments.get(k)==null){
                arguments.put(k,v.getDefaultValue());
            }
        });

        // 检查必需参数
        if (!parameters.getRequired().isEmpty()) {
            List<String> required = parameters.getRequired();
            for (String req : required) {
                if (!arguments.containsKey(req)) {
                    throw new IllegalArgumentException("Missing required argument: " + req);
                }
            }
        }
    }

    private void sendStandardResponse(RoutingContext ctx, McpRequest request, Object result) {
        McpResponse response = new McpResponse();
        response.setId(request.getId());
        response.setResult(result);

        ctx.response()
                .putHeader("Content-Type", "application/json")
                .putHeader("X-Response-Time", String.valueOf(System.currentTimeMillis()))
                .end(Json.encode(response));
    }

    private void sendStreamingResponse(RoutingContext ctx, McpRequest request, Iterable<?> result)  {
        // 流式响应实现
        String taskId = request.getId();
        List<JsonObject> content = new ArrayList<>();
        JsonObject jsonResult = new JsonObject().put("content",content).put("isPartial",true);
        // root消息
        JsonObject rootMsg = new JsonObject()
                .put("jsonrpc", "2.0")
                .put("id", taskId)
                .put("result", jsonResult);

        int sequence = 0;
        long startTime = System.currentTimeMillis();

        // 模拟流式数据处理
        for(Object row: result) {
            try {
                if (row instanceof Future<?>){
                    row = ((Future)row).result();
                }
                if (row instanceof IgniteFuture<?>){
                    row = ((IgniteFuture)row).get();
                }
                if (row instanceof java.util.concurrent.Future){
                    row = ((java.util.concurrent.Future)row).get();
                }

            } catch (Exception e) {
                LOGGER.warning("Get Iterable result failed:"+e.getMessage());
                jsonResult.put("isPartial",false);
                jsonResult.put("isError",true);
                JsonObject dataMsg = new JsonObject()
                        .put("type", "text")
                        .put("message", e.getMessage())
                        .put("code", -32000)
                        .put("sequence", sequence);

                content.clear();
                content.add(dataMsg);
                sendStreamEvent(ctx, null, rootMsg);

                ctx.response().end();
                return;
            }

            if (row instanceof byte[]) {
                JsonObject dataMsg = new JsonObject()
                        .put("type", "data")
                        .put("data", row)
                        .put("sequence", sequence);

                content.clear();
                content.add(dataMsg);
                sendStreamEvent(ctx, null, rootMsg);
            } else if (row instanceof JsonObject) {
                JsonObject dataMsg = new JsonObject()
                        .put("type", "data")
                        .put("data", row)
                        .put("sequence", sequence);

                content.clear();
                content.add(dataMsg);
                sendStreamEvent(ctx, null, rootMsg);
            } else if(row instanceof Map.Entry){
                Map.Entry<String,Object> entry = (Map.Entry)row;
                JsonObject dataMsg = new JsonObject()
                        .put("type", "data")
                        .put(entry.getKey(), entry.getValue())
                        .put("sequence", sequence);

                content.clear();
                content.add(dataMsg);
                sendStreamEvent(ctx, null, rootMsg);
            }
            else {
                JsonObject dataMsg = new JsonObject()
                        .put("type", "text")
                        .put("text", row.toString())
                        .put("sequence", sequence);

                content.clear();
                content.add(dataMsg);
                sendStreamEvent(ctx, null, rootMsg);
            }
            sequence++;
        };

        // 发送完成消息
        jsonResult.put("isPartial",false);
        JsonObject completeMsg = new JsonObject()
                .put("type", "complete")
                .put("status", "success")
                .put("duration", System.currentTimeMillis() - startTime);
        content.clear();
        content.add(completeMsg);
        sendStreamEvent(ctx, null, rootMsg);

        ctx.response().end();

    }

    private void sendError(RoutingContext ctx, String requestId, String message, int code) {
        McpError error = new McpError(code,message,null);

        McpResponse response = new McpResponse();
        response.setId(requestId);
        response.setError(error);

        int httpStatus = determineHttpStatus(code);
        ctx.response()
                .setStatusCode(httpStatus)
                .putHeader("Content-Type", "application/json")
                .end(Json.encode(response));
    }

    private int determineHttpStatus(int errorCode) {
        switch (errorCode) {
            case -32700: return 400; // Parse error
            case -32600: return 400; // Invalid request
            case -32601: return 404; // Method not found
            case -32602: return 400; // Invalid params
            default: return 500;
        }
    }

    private void registerBuiltinTools() {
        // 注册天气查询工具
        ToolExecutor weatherTool = ToolExecutor.builder()
                .name("get_weather")
                .description("Get weather information for a city")
                .parameter("city", "string", "City name", true)
                .parameter("date", "string", "Date in YYYY-MM-DD format", false)
                .streamingSupported(false)
                .executor(args -> {
                    String city = (String) args.get("city");
                    String date = (String) args.getOrDefault("date", "today");

                    Map<String, Object> result = new HashMap<>();
                    result.put("city", city);
                    result.put("date", date);
                    result.put("temperature", "20°C");
                    result.put("condition", "Sunny");
                    result.put("humidity", "65%");
                    result.put("windSpeed", "10 km/h");
                    return result;
                })
                .build();
        registerTool(weatherTool);

        // 注册加法计算工具
        ToolExecutor sumTool = ToolExecutor.builder()
                .name("calculate_sum")
                .description("Calculate sum of two numbers")
                .parameter("a", "number", "First number", true)
                .parameter("b", "number", "Second number", true)
                .streamingSupported(false)
                .executor(args -> {
                    Integer a = ((Number) args.get("a")).intValue();
                    Integer b = ((Number) args.get("b")).intValue();

                    Map<String, Object> result = new HashMap<>();
                    result.put("a", a);
                    result.put("b", b);
                    result.put("sum", a + b);
                    result.put("timestamp", System.currentTimeMillis());
                    return result;
                })
                .build();
        registerTool(sumTool);

        // 注册长耗时任务示例（支持流式）
        ToolExecutor longRunningTool = ToolExecutor.builder()
                .name("process_data")
                .description("Process large dataset with progress updates")
                .parameter("dataset", "string", "Dataset identifier", true)
                .parameter("batchSize", "number", "Batch size", false)
                .streamingSupported(true)
                .executor(args -> {
                    // 简化实现，实际使用时应该实现StreamingToolExecutor接口
                    Map<String, Object> result = new HashMap<>();
                    result.put("status", "completed");
                    result.put("processed", 1000);
                    result.put("duration", 5000);
                    return result;
                })
                .build();
        registerTool(longRunningTool);

        realtimeDataStreamTool = new RealtimeDataStreamTool(vertx);
        registerStreamingTool(realtimeDataStreamTool);

        StreamFileProcessorTool fileProcessorTool = new StreamFileProcessorTool(vertx,this.igniteInstanceName);
        registerStreamingTool(fileProcessorTool);
    }

    private void loadToolsFromSpringContext() {
        if (springContext != null) {
            Map<String, ToolExecutor> springTools = springContext.getBeansOfType(ToolExecutor.class);
            for (ToolExecutor tool : springTools.values()) {
                registerTool(tool);
                LOGGER.info("Loaded tool from Spring context: " + tool.getName());
            }
        }
    }

    // 公共API方法
    public void registerTool(ToolExecutor tool) {
        if (tool == null || tool.getName() == null) {
            throw new IllegalArgumentException("Tool and tool name cannot be null");
        }
        toolRegistry.put(tool.getName(), tool);
        LOGGER.info("Registered tool: " + tool.getName());

        // 通知工具列表变更
        notifyToolsListChanged();
    }

    // 在 StreamableMcpServer 中添加方法
    public void registerStreamingTool(StreamingToolExecutor tool) {
        toolRegistry.put(tool.getName(), tool);
        LOGGER.info("Registered streaming tool: " + tool.getName());
        notifyToolsListChanged();
    }

    public void unregisterTool(String toolName) {
        ToolExecutor removed = toolRegistry.remove(toolName);
        if (removed != null) {
            LOGGER.info("Unregistered tool: " + toolName);
            notifyToolsListChanged();
        }
    }

    public void addToolInterceptor(ToolInterceptor interceptor) {
        toolInterceptors.add(interceptor);
        LOGGER.info("Added tool interceptor: " + interceptor.getClass().getSimpleName());
    }

    public void removeToolInterceptor(ToolInterceptor interceptor) {
        toolInterceptors.remove(interceptor);
    }

    private void notifyToolsListChanged() {
        // 通知所有活跃会话工具列表已变更
        sessionManager.notifyToolsListChanged();
    }

    public void setRequestLogger(RequestLogger logger) {
        this.requestLogger = logger;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public Map<String, ToolExecutor> getRegisteredTools() {
        return Collections.unmodifiableMap(toolRegistry);
    }

    public ServerCapabilities getServerCapabilities() {
        return serverCapabilities;
    }

    private boolean isStreamingTool(ToolExecutor tool) {
        return tool != null && tool.isStreamingSupported();
    }

    private Map<String, String> parameters(HttpServerRequest req) {
        MultiMap params = req.params();

        if (F.isEmpty(params))
            return Collections.emptyMap();

        Map<String, String> map = U.newHashMap(params.size());

        for (Map.Entry<String, String> entry : req.params())
            map.put(entry.getKey(), entry.getValue());

        return map;
    }

    private static class RequestLogger {
        private final io.vertx.core.Vertx vertx;
        private final java.util.concurrent.ConcurrentLinkedQueue<String> logs = new java.util.concurrent.ConcurrentLinkedQueue<>();

        public RequestLogger(io.vertx.core.Vertx vertx) {
            this.vertx = vertx;
        }

        public void logRequest(io.vertx.core.http.HttpServerRequest request, String body) {
            String log = String.format("[%d] %s %s - body length: %d",
                    System.currentTimeMillis(),
                    request.method(),
                    request.path(),
                    body != null ? body.length() : 0);
            logs.add(log);

            // 限制日志队列大小
            while (logs.size() > 1000) {
                logs.poll();
            }
        }
    }
}
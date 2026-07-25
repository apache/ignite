package io.vertx.webmvc.mcp;

/**
 * 工具拦截器接口
 */
public interface ToolInterceptor {
    default boolean beforeExecute(ToolExecutionContext context) { return true; }
    default void afterExecute(ToolExecutionContext context, Object result) {}
    default void onError(ToolExecutionContext context, Throwable error) {}
}

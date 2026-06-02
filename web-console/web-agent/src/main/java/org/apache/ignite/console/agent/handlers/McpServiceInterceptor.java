package org.apache.ignite.console.agent.handlers;

import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.mcp.ToolExecutionContext;
import io.vertx.webmvc.mcp.ToolInterceptor;
import org.apache.ignite.console.agent.service.ServiceResult;

import java.util.List;
import java.util.Map;

public class McpServiceInterceptor implements ToolInterceptor {
    @Override
    public void afterExecute(ToolExecutionContext context, Object result){
        if(result instanceof ServiceResult){
            boolean isStructOutput = context.isStructuredOutput();
            ServiceResult serviceResult = (ServiceResult) result;
            Map<String,Object> res = serviceResult.getResult();
            if(serviceResult.getErrorType()!=null && isStructOutput){
                context.setExecutedResult(res);
            }
            else if(!isStructOutput){
                context.setExecutedResult(serviceResult.getMessages());
            }
            else{
                context.setExecutedResult(serviceResult.toJson());
            }
        }
    }
}

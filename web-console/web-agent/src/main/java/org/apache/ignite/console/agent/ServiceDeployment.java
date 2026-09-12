package org.apache.ignite.console.agent;
import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;
import io.vertx.webmvc.mcp.StreamableMcpServer;
import io.vertx.webmvc.mcp.ToolExecutor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.service.*;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import static io.vertx.webmvc.mcp.McpSchema.*;

public class ServiceDeployment {
    private static final IgniteLogger logger = new Slf4jLogger(LoggerFactory.getLogger(ServiceDeployment.class));
    static Map<String, List<McpToolInfo>> toolsInfo = new ConcurrentHashMap<>();
    static Map<String, ToolExecutor> toolExecutorMap = new ConcurrentHashMap<>();

    public static List<Class<?>> scanWithSpring(String basePackage) throws IOException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        MetadataReaderFactory metaReader = new CachingMetadataReaderFactory();

        String packagePath = basePackage.replace('.', '/');
        Resource[] resources = resolver.getResources("classpath*:" + packagePath + "/**.class");

        List<Class<?>> result = new ArrayList<>();
        for (Resource resource : resources) {
            MetadataReader reader = metaReader.getMetadataReader(resource);
            String className = reader.getClassMetadata().getClassName();

            try {
                Class<?> clazz = Class.forName(className);
                if (clazz.isAnnotationPresent(ApiOperation.class)) {
                    result.add(clazz);
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        return result;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param ignite Distributed services on the grid.
     */
    public static void deployBuildinServices(Ignite ignite) {
        CompletableFuture.runAsync(()->{

        var services = ignite.services();
        IgniteVertxPlugin vertxPlugin = ignite.plugin("Vertx");
        StreamableMcpServer mcpServer = vertxPlugin.starter().findVerticle(StreamableMcpServer.class);
        while(mcpServer==null){
            try {
                Thread.sleep(1000);
                mcpServer = vertxPlugin.starter().findVerticle(StreamableMcpServer.class);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        List<Class<?>> serviceList = null;
        try {
            serviceList = scanWithSpring("org.apache.ignite.console.agent.service");
            for(Class svc: serviceList){
                String svcName = svc.getSimpleName();

                try {
                    ApiOperation api = (ApiOperation) svc.getAnnotation(ApiOperation.class);
                    if(api.hidden()){
                        continue;
                    }
                    if(api.nickname()!=null && !api.nickname().isBlank()){
                        svcName = api.nickname();
                    }
                    Service svcInstance = (Service)svc.getDeclaredConstructor().newInstance();
                    if(CacheAgentService.class.isAssignableFrom(svc)){
                        CacheAgentService cacheSvc = (CacheAgentService)svcInstance;
                        services.deployNodeSingleton(svcName,cacheSvc);
                    }
                    else if(ClusterAgentService.class.isAssignableFrom(svc)){
                        ClusterAgentService clusterSvc = (ClusterAgentService)svcInstance;
                        services.deployClusterSingleton(svcName,clusterSvc);
                    }
                    else if(Service.class.isAssignableFrom(svc)){
                        services.deployNodeSingleton(svcName,svcInstance);
                    }

                    if(mcpServer!=null){
                        if(svcInstance instanceof McpService){
                            McpService mcpService = ignite.services().service(svcName);
                            registerToolList(mcpServer,mcpService,svcName);
                        }
                    }

                } catch (InstantiationException | NoSuchMethodException | InvocationTargetException |
                         IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            //String cacheName = "default";
            //services.deployKeyAffinitySingleton("loadDataKeyAffinityService",new ClusterLoadDataService(), cacheName, "id");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        });

    }
    /**
     * deploy service
     */
    public static ServiceResult deployService(Ignite ignite,ServiceConfiguration cfg,String serviceCls,String mode) {

        ServiceResult result = new ServiceResult();

        try {
            JsonObject info = new JsonObject();
            // must be java bean
            Class<? extends Service> svcCls = (Class<? extends Service>) Class.forName(serviceCls);
            Service svc = svcCls.getDeclaredConstructor().newInstance();

            ApiOperation api = svcCls.getAnnotation(ApiOperation.class);
            if(cfg.getName()==null){
                if(api.nickname()!=null){
                    cfg.setName(api.nickname());
                }
                else {
                    cfg.setName(svcCls.getName());
                }
            }
            info.put("name", cfg.getName());
            if (api != null) {
                info.put("description", api.value());
                info.put("notes", api.notes());

            } else {
                info.put("description", svcCls.getName());
                info.put("notes", "");
            }
            info.put("cacheName", cfg.getCacheName());
            info.put("mode", mode);

            cfg.setService(svc);
            ignite.services().deploy(cfg);

            if(svc instanceof McpService){
                IgniteVertxPlugin vertxPlugin = ignite.plugin("Vertx");
                StreamableMcpServer mcpServer = vertxPlugin.starter().findVerticle(StreamableMcpServer.class);
                McpService mcpService = ignite.services().service(cfg.getName());
                registerToolList(mcpServer,mcpService,cfg.getName());

                info.put("tools", getToolList(cfg.getName()));
            }

        } catch (Exception e) {
            result.addMessage(e.getMessage());
        }

        return result;

    }

    public static List<McpToolInfo> registerToolList(StreamableMcpServer mcpServer,McpService mcpService,String serviceName){
        List<McpToolInfo> toolInfos = new ArrayList<>();
        for(ToolExecutor toolEx: mcpService.toolExecutors()){
            mcpServer.registerTool(toolEx);
            McpToolInfo toolInfo = new McpToolInfo();
            toolInfo.setName(toolEx.getName());
            toolInfo.setDescription(toolEx.getDescription());
            toolInfo.setInputSchema(toolEx.getParameters());
            toolInfo.setOutputSchema(toolEx.getOutputSchema());

            ToolMeta meta = new ToolMeta();
            meta.setIsStreaming(toolEx.isStreamingSupported());
            toolInfo.setMeta(meta);
            toolInfos.add(toolInfo);

            var oldInfo = toolExecutorMap.put(toolInfo.getName(), toolEx);
            if(oldInfo!=null){
                logger.warning("Tool name is "+toolInfo.getName()+" already existed!");
            }
        }
        // 缓存toolInfo
        toolsInfo.put(serviceName,toolInfos);
        return toolInfos;
    }

    public static List<McpToolInfo> getToolList(String name){
        return toolsInfo.get(name);
    }

    public static ToolExecutor getToolExecutor(String name){
        return toolExecutorMap.get(name);
    }

}

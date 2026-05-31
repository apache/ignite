package org.apache.ignite.console.mcp;

import io.modelcontextprotocol.server.*;
import static io.modelcontextprotocol.server.McpServerFeatures.*;

import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.console.web.security.TokenAuthentication;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import org.springframework.core.env.Environment;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;


import java.util.*;
import java.util.stream.Collectors;

import static io.modelcontextprotocol.spec.McpSchema.*;

@Component
public class MCPResourceProvider implements SmartInitializingSingleton {
    @Autowired
    private Environment env;
    @Autowired
    private ApplicationContext context;
    @Autowired
    private ConfigurationsService repo;
    @Autowired
    private AccountsService accountsService;


    private McpSyncServer getMcpSyncServer() {
        // 确保在 McpSyncServer 构建完成后才调用
        return context.getBean(McpSyncServer.class);
    }

    @Override
    public void afterSingletonsInstantiated() {
        // 注册一个带有动态参数 accountId 的资源模板
        McpSchema.Resource db = McpSchema.Resource.builder()
                .uri("databases")  // URI模板
                .name("User Databases")
                .description("获取当前用户可以访问的数据库列表")
                .mimeType("application/json")
                .build();

        SyncResourceSpecification spec
                = new SyncResourceSpecification(db,this::handleUserResourceRequest);

        getMcpSyncServer().addResource(spec);
    }

    // 动态处理器：根据 accountId 返回不同的资源列表
    private McpSchema.ReadResourceResult handleUserResourceRequest(McpSyncServerExchange exchange, McpSchema.ReadResourceRequest request) {

        List<ResourceContents> contents = new ArrayList<>();
        Map<String,Object> gMeta = new HashMap<>();
        // 根据 userId 查询数据库或业务逻辑
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if(auth!=null && auth instanceof TokenAuthentication){
            TokenAuthentication tokenAuth = (TokenAuthentication) auth;
            Account account = tokenAuth.getPrincipal();
            if(account!=null) {
                // 动态构建资源列表
                JsonArray clusters = repo.loadClusters(new ConfigurationKey(account.getId(), isTestEnv()));
                for(Object item: clusters){
                    JsonObject cluster = (JsonObject)item;
                    String jdbc = "jdbc:ignite:thin://127.0.0.1:10802/"+cluster.getString("name");
                    String uri = request.uri()+":"+cluster.getString("name");
                    Map<String,Object> dbMeta = new HashMap<>();
                    dbMeta.put("jdbc",jdbc);
                    var resource = new TextResourceContents(
                            uri, "application/json", cluster.toString(),dbMeta
                    );
                    contents.add(resource);
                }
            }
        }

        return new McpSchema.ReadResourceResult(contents,gMeta);
    }



    @Bean
    public List<McpServerFeatures.SyncPromptSpecification> myPrompts() {
        var prompt = new Prompt("er_diagram", "The assistants goal is to use the MCP server to create a visual ER diagram of the database.",
                List.of(new PromptArgument("name", "The name to database", true)));

        var promptSpecification = new McpServerFeatures.SyncPromptSpecification(prompt, (exchange, getPromptRequest) -> {
            String nameArgument = (String) getPromptRequest.arguments().get("name");
            if (nameArgument == null) { nameArgument = "demo"; }
            var userMessage = new PromptMessage(Role.USER, new TextContent("Hello " + nameArgument + "! How can I assist you today?"));
            return new GetPromptResult("Visualize ER diagram", List.of(userMessage));
        });

        return List.of(promptSpecification);
    }

    public boolean isTestEnv(){
        Optional<String> profile = Arrays.stream(env.getActiveProfiles()).filter(p->p.equals("test")).findFirst();
        if(profile.isPresent() && profile.get().equals("test")){
            return true;
        }
        return false;
    }
}

package org.apache.ignite.console.mcp;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.apache.ignite.console.web.security.TokenAuthentication;
import org.springaicommunity.mcp.annotation.McpTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.*;

@Configuration
public class MCPToolsProvider {

    @Autowired
    private Environment env;

    @Autowired
    private ConfigurationsService repo;

    //@Bean
    public List<McpServerFeatures.SyncToolSpecification> streamableHttpTools() {
        List<McpServerFeatures.SyncToolSpecification> tools = new ArrayList<>();
        String status = "published";
        if(isTestEnv()){
            status = "draft";
        }
        // 你可以在这里添加更多HTTP专用的工具
        return tools;
    }

    @McpTool(description = "Get database(catalog) list about current user.")
    public List<McpSchema.ResourceContents> userDatabaseList() {

        List<McpSchema.ResourceContents> contents = new ArrayList<>();
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
                    String uri = "database:"+cluster.getString("name");
                    Map<String,Object> dbMeta = new HashMap<>();
                    dbMeta.put("jdbc",jdbc);
                    var resource = new McpSchema.TextResourceContents(
                            uri, "application/json", cluster.toString(),dbMeta
                    );
                    contents.add(resource);
                }
            }
        }

        return contents;
    }

    public boolean isTestEnv(){
        Optional<String> profile = Arrays.stream(env.getActiveProfiles()).filter(p->p.equals("test")).findFirst();
        if(profile.isPresent() && profile.get().equals("test")){
            return true;
        }
        return false;
    }

}

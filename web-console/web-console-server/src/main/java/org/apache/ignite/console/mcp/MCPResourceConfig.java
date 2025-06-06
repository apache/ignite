package org.apache.ignite.console.mcp;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.web.model.ConfigurationKey;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

import static io.modelcontextprotocol.spec.McpSchema.*;

@Configuration
public class MCPResourceConfig {

    @Value("${mcp-server.user-email}")
    String mcp_user_email;


    @Bean
    public List<McpServerFeatures.SyncResourceSpecification> mySyncResourceSpecification(ConfigurationsService repo, AccountsService accountsService) {
        List<McpServerFeatures.SyncResourceSpecification> list = new ArrayList<>();
        Account account = accountsService.loadUserByUsername(mcp_user_email);
        if(account!=null){
            JsonArray clusters = repo.loadClusters(new ConfigurationKey(account.getId(),false));
            for(Object item: clusters){
                JsonObject cluster = (JsonObject)item;
                String jdbc = "jdbc:ignite:thin://127.0.0.1:10802/"+cluster.getString("name");
                var resource = new Resource(
                        jdbc,
                        cluster.getString("name"),
                        cluster.getString("comment"),
                        "database", null
                        );

                var resourceSpecification = new McpServerFeatures.SyncResourceSpecification(resource, (exchange, getResourceRequest) -> {
                    String uri = getResourceRequest.uri();
                    var contents = new TextResourceContents(uri, "database", "<table list>");
                    return new ReadResourceResult(List.of(contents));
                });
            }

        }

        return list;
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
}

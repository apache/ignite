package org.apache.ignite.console.mcp;

import io.modelcontextprotocol.server.McpServerFeatures;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.context.annotation.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Configuration
public class MCPToolsProvider {

    @Autowired
    private Environment env;

    //@Bean
    public List<McpServerFeatures.AsyncToolSpecification> streamableHttpTools() {
        List<McpServerFeatures.AsyncToolSpecification> tools = new ArrayList<>();
        String status = "published";
        if(isTestEnv()){
            status = "draft";
        }
        // 你可以在这里添加更多HTTP专用的工具
        return tools;
    }

    public boolean isTestEnv(){
        Optional<String> profile = Arrays.stream(env.getActiveProfiles()).filter(p->p.equals("test")).findFirst();
        if(profile.isPresent() && profile.get().equals("test")){
            return true;
        }
        return false;
    }

}

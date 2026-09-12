package org.apache.ignite.console.mcp;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import org.apache.ignite.IgniteCheckedException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.TimeUnit;
//@RunWith(SpringRunner.class)
//@SpringBootTest
public class McpServiceQuery{

    private static final Logger logger = LoggerFactory.getLogger(McpServiceQuery.class);

    //@MockBean
    private McpSyncClient mcpSyncClient;



    /**
     * 从客户端查询（作为 MCP 客户端）
     */
    //@Test
    public void testQueryFromClient() {
        try {
            logger.info("正在通过 MCP 客户端查询...");

            McpClientConfig cfg = new McpClientConfig();
            mcpSyncClient = cfg.mcpSyncClient();
            // 1. 查询资源列表
            McpSchema.ListResourcesResult resourcesResult = mcpSyncClient.listResources();
            List<McpSchema.Resource> resources = resourcesResult.resources();
            logger.info("========== 资源列表 (共 {} 个) ==========", resources.size());
            for (McpSchema.Resource resource : resources) {
                logger.info("资源名称: {}", resource.name());
                logger.info("资源URI: {}", resource.uri());
                logger.info("资源描述: {}", resource.description());
                logger.info("MIME类型: {}", resource.mimeType());
                logger.info("---");
            }

            // 2. 查询工具列表
            McpSchema.ListToolsResult toolsResult = mcpSyncClient.listTools();
            List<McpSchema.Tool> tools = toolsResult.tools();
            logger.info("========== 工具列表 (共 {} 个) ==========", tools.size());
            for (McpSchema.Tool tool : tools) {
                logger.info("工具名称: {}", tool.name());
                logger.info("工具描述: {}", tool.description());
                logger.info("输入Schema: {}", tool.inputSchema());
                logger.info("---");
            }

            // 3. 查询提示词列表
            McpSchema.ListPromptsResult promptsResult = mcpSyncClient.listPrompts();
            List<McpSchema.Prompt> prompts = promptsResult.prompts();
            logger.info("========== 提示词列表 (共 {} 个) ==========", prompts.size());
            for (McpSchema.Prompt prompt : prompts) {
                logger.info("提示词名称: {}", prompt.name());
                logger.info("提示词描述: {}", prompt.description());
                if (prompt.arguments() != null) {
                    logger.info("参数列表: {}", prompt.arguments());
                }
                logger.info("---");
            }

        } catch (Exception e) {
            logger.error("查询失败: {}", e.getMessage(), e);
        }
    }

    public static void main(String... args){
        McpServiceQuery test = new McpServiceQuery();
        test.testQueryFromClient();
    }

}

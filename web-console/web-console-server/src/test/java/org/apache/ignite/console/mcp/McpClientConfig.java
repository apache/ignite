package org.apache.ignite.console.mcp;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;

import io.modelcontextprotocol.spec.McpSchema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;

/**
 * MCP 客户端配置 - 用于主动查询服务信息
 */
@Configuration
public class McpClientConfig {

    /**
     * 创建 MCP 同步客户端（通过 HTTP 连接）
     */
    @Bean
    @Primary
    public McpSyncClient mcpSyncClient() {
        // HTTP 传输配置
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder("http://localhost:3000").connectTimeout(Duration.ofSeconds(300))
                .endpoint("/mcp")
                .build();

        // 创建客户端
        McpSyncClient client = McpClient.sync(transport)
                .clientInfo(new McpSchema.Implementation("test-client", "1.0.0"))
                .capabilities(
                        McpSchema.ClientCapabilities.builder()
                                .roots(true)
                                .sampling()
                                .build()
                )
                .requestTimeout(Duration.ofSeconds(30))
                .build();

        // 初始化连接
        client.initialize();

        return client;
    }

}

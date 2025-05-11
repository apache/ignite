package org.apache.ignite.console.web.socket;

import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurationSupport;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSERS_PATH;

import javax.websocket.WebSocketContainer;

/**
 * Websocket configuration.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    /** */
    private final AgentsService agentsSrvc;

    /** */
    private final BrowsersService browsersSrvc;    
   
    /**
     * @param agentsSrvc Agents service.
     * @param browsersSrvc Browsers service.
     */
    public WebSocketConfig(AgentsService agentsSrvc, BrowsersService browsersSrvc) {
        this.agentsSrvc = agentsSrvc;
        this.browsersSrvc = browsersSrvc;
    }

    /**
     * @param registry Registry.
     */
    @Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(agentsSrvc, AGENTS_PATH).setAllowedOrigins("*");
        registry.addHandler(browsersSrvc, BROWSERS_PATH).setAllowedOrigins("*");      
        
    }  
 
}

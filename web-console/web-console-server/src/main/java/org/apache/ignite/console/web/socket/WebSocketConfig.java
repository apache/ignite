/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.socket;

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

import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSERS_PATH;

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
        registry.addHandler(agentsSrvc, AGENTS_PATH);//.setAllowedOrigins("*");
        registry.addHandler(browsersSrvc, BROWSERS_PATH);//.setAllowedOrigins("*");
    }
    
    
    @Bean
    public TaskScheduler taskScheduler(){
      ThreadPoolTaskScheduler scheduler=new ThreadPoolTaskScheduler();
      scheduler.setPoolSize(4);
      scheduler.initialize();
      return scheduler;
    }
 
}

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

package org.apache.ignite.agent.config;

import java.util.List;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.binaryMapper;

/**
 * Websocket configuration for test Spring Boot application.
 */
@TestConfiguration
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {
    /** Agent path. */
    public static final String AGENTS_PATH = "/agents";

    /** {@inheritDoc} */
    @Override public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint(AGENTS_PATH);
    }

    /** {@inheritDoc} */
    @Override public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        registration.setMessageSizeLimit(128 * 1024);
    }

    /** {@inheritDoc} */
    @Override public void configureMessageBroker(MessageBrokerRegistry cfg) {
        cfg.setApplicationDestinationPrefixes("/app");
        cfg.enableSimpleBroker("/topic");
    }

    /** {@inheritDoc} */
    @Override public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(testInterceptor());
    }

    /**
     * Increase messages size.
     */
    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();

        container.setMaxTextMessageBufferSize(131072);
        container.setMaxBinaryMessageBufferSize(131072);

        return container;
    }

    /** {@inheritDoc} */
    @Override public boolean configureMessageConverters(List<MessageConverter> msgConverters) {
        MappingJackson2MessageConverter smileJsonConverter = new MappingJackson2MessageConverter(MimeTypeUtils.APPLICATION_OCTET_STREAM);

        smileJsonConverter.setObjectMapper(binaryMapper());

        msgConverters.add(smileJsonConverter);

        return false;
    }

    /**
     * Test channel interceptor.
     */
    @Bean
    public TestChannelInterceptor testInterceptor() {
        return new TestChannelInterceptor();
    }

    /**
     * @return Scheduler for STOMP heartbeats.
     */
    protected TaskScheduler getHeartbeatScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("stomp-heartbeat-thread-");
        scheduler.initialize();

        return scheduler;
    }
}

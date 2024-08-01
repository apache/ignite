package org.apache.ignite.console.web.socket;

import javax.websocket.WebSocketContainer;

import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
public class WebSocketMessageBrokerConfig implements WebSocketMessageBrokerConfigurer {
	public static int MSG_SIZE = 10*1024*1024;
	
	

	@Override
	public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
		registry.setMessageSizeLimit(MSG_SIZE);
		registry.setSendBufferSizeLimit(MSG_SIZE);
	}
	
	@Override
	public void configureClientInboundChannel(ChannelRegistration registration) {
		
	}

	
	@Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        // 在此处设置bufferSize        
        container.setMaxTextMessageBufferSize(MSG_SIZE);
        container.setMaxBinaryMessageBufferSize(MSG_SIZE);
        container.setMaxSessionIdleTimeout(60 * 60000L);
        return container;
    }

}

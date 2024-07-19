package org.apache.ignite.console.web.socket;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

@Configuration
public class WebSocketMessageBrokerConfig implements WebSocketMessageBrokerConfigurer {
	public static int MSG_SIZE = 1024*1024;

	public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
		registry.setMessageSizeLimit(MSG_SIZE);
		registry.setSendBufferSizeLimit(MSG_SIZE);		
	}
}

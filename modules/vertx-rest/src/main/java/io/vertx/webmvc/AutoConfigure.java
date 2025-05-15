package io.vertx.webmvc;

import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import io.vertx.webmvc.starter.VertXStarter;

/**
 * AutoConfigure
 * @author zjf
 */
@Configuration
@ComponentScan(basePackages = "io.vertx.webmvc")
@PropertySource("classpath:application.properties")
public class AutoConfigure {
	
	@Value("${spring.web.contexPath:}")
	String contexPath;
    
	@Bean
    //@ConditionalOnMissingBean
    public VertXStarter vertxStarterBean() {
        return new VertXStarter(contexPath);
    }
}

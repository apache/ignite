package io.vertx.webmvc;

//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import io.vertx.webmvc.starter.VertXStarter;

/**
 * AutoConfigure
 * @author zbw
 */
@Configuration
@ComponentScan(basePackages = "io.vertx.webmvc")
public class AutoConfigure {
    @Bean
    //@ConditionalOnMissingBean
    public VertXStarter vertxStarterBean() {
        return new VertXStarter();
    }
}

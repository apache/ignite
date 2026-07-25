

package org.apache.ignite.console.config;

import java.util.Map;

import org.apache.ignite.console.web.security.PassportLocalPasswordEncoder;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import jakarta.annotation.PostConstruct;

/**
 * Application configuration.
 **/
@Configuration
@EnableScheduling
public class ApplicationConfiguration  {
	

    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        String encodingId = "bcrypt";

        Map<String, PasswordEncoder> encoders = F.asMap(
            encodingId, new BCryptPasswordEncoder(),
            "pbkdf2", new PassportLocalPasswordEncoder()
        );

        return new DelegatingPasswordEncoder(encodingId, encoders);
    }

    /**
     * @return Application event multicaster.
     */
    @Bean(name = "applicationEventMulticaster")
    public ApplicationEventMulticaster simpleApplicationEventMulticaster(TaskExecutor executor) {
        SimpleApplicationEventMulticaster evtMulticaster = new SimpleApplicationEventMulticaster();

        evtMulticaster.setTaskExecutor(getAsyncExecutor());
        return evtMulticaster;
    }

    /**
     * Thread pool task executor.
     */
    @Bean    
    public TaskExecutor getAsyncExecutor() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setMaxPoolSize(Math.max(8, Runtime.getRuntime().availableProcessors()));
        pool.initialize();

        return pool;
    }    
    
    @Bean
    public TaskScheduler taskScheduler(){
      ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
      scheduler.setPoolSize(4);      
      scheduler.initialize();
      
      return scheduler;
    }    
   
}

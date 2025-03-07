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

package org.apache.ignite.console.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.Servlet;

import org.apache.ignite.Ignite;
import org.apache.ignite.console.web.security.PassportLocalPasswordEncoder;
import org.apache.ignite.internal.util.typedef.F;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.Ordered;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;

/**
 * Application configuration.
 **/
@Configuration
@EnableScheduling
public class ApplicationConfiguration  {
	
	@Value("${quercus.webapp.dir:}")
    private String quercusWebappDir = "";

    @PostConstruct
    public void init() {
        System.setProperty("quercus.webapp_dir", quercusWebappDir);
    }
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
    
    @Bean
    public ServletRegistrationBean<Servlet> customServletRegistration() {
    	String servletName = "com.caucho.quercus.servlet.QuercusServlet";
    	
		try {
			Class<Servlet> servlet = (Class<Servlet>) Class.forName(servletName);
			Servlet servletInstance = servlet.getConstructor().newInstance();
			ServletRegistrationBean<Servlet> registration =  new ServletRegistrationBean<>(servletInstance, "*.php");
	        //registration.addInitParameter("ini-file", "file:config/php.ini");
	        registration.addInitParameter("script-encoding", "utf-8");
	        registration.setOrder(Ordered.HIGHEST_PRECEDENCE); // 设置高优先级
	        registration.setName("Quercus Servlet");
	        return registration;
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		DefaultServlet servletInstance = new DefaultServlet();		
		return new ServletRegistrationBean<>(servletInstance, "/static");
    	
    }
}

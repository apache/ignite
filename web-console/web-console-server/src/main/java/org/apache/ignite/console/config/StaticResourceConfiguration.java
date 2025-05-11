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

import java.util.Collections;
import java.util.List;

import org.apache.ignite.console.common.VertxJsonHttpMessageConverter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;


/**
 * Static server configuration.
 */
@Configuration
public class StaticResourceConfiguration implements WebMvcConfigurer {
    /** Application context. */
    private final ApplicationContext applicationCtx;    
    

    /**
     * @param applicationCtx Application context.
     */
    public StaticResourceConfiguration(ApplicationContext applicationCtx) {
        this.applicationCtx = applicationCtx;
    }

    /** {@inheritDoc} */
    @Override public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("swagger-ui.html")
            .addResourceLocations("classpath:/META-INF/resources/");

        registry.addResourceHandler("/webjars/**")
            .addResourceLocations("classpath:/META-INF/resources/webjars/");
        
        registry.addResourceHandler("/phpMongoAdmin/**").
        	addResourceLocations("file:phpMongoAdmin/");

        registry.addResourceHandler("/**").
            addResourceLocations("file:frontend/");
        
        
    }
    
    /**
     * SpringBoot设置首页
     */
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
    	
    	 // Map "/"
        registry.addViewController("/").setViewName("forward:/index.html");

        // Single directory level - no need to exclude "api"
        registry.addViewController("/{x:[\\w\\-]+}").setViewName("forward:/index.html");

        // Multi-level directory path, need to exclude "api" on the first part of the path
        registry.addViewController("/{x:^(?!api$).*$}/**/{y:[\\w\\-]+}").setViewName("forward:/index.html");
        
        WebMvcConfigurer.super.addViewControllers(registry);
        
        //registry.setOrder(Ordered.HIGHEST_PRECEDENCE);
    }
    
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
        		.maxAge(3600)
                .allowedOriginPatterns("*")                        
                .allowCredentials(true)
                .allowedHeaders("*")
                .exposedHeaders("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS");
    }   

    /**
     * @return Favicon mapping.
     */
    @Bean
    public SimpleUrlHandlerMapping customFaviconHandlerMapping() {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();

        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE);
        mapping.setUrlMap(Collections.singletonMap("**/favicon.ico", customFaviconRequestHandler()));

        return mapping;
    }

    /** 
     * @return Favicon request handler.
     */
    @Bean
    protected ResourceHttpRequestHandler customFaviconRequestHandler() {
        ResourceHttpRequestHandler reqHnd = new ResourceHttpRequestHandler();

        reqHnd.setLocations(Collections.singletonList(applicationCtx.getResource("file:frontend/favicon.ico")));

        return reqHnd;
    }
    
    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {

    	//1.需要定义一个convert转换消息的对象;
    	VertxJsonHttpMessageConverter jsonHttpMessageConverter = new VertxJsonHttpMessageConverter();
    	jsonHttpMessageConverter.setObjectMapper(DatabindCodec.mapper());
        converters.add(0,jsonHttpMessageConverter);
    }
}

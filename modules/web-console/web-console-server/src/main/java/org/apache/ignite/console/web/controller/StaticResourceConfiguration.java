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

package org.apache.ignite.console.web.controller;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

/**
 * Static server configuration.
 */
@Configuration
public class StaticResourceConfiguration extends WebMvcConfigurerAdapter {
    /** */
    private static final Logger log = LoggerFactory.getLogger(StaticResourceConfiguration.class);

    /** Application context. */
    private final ApplicationContext applicationCtx;

    /** Frontend folder url. */
    private URL frontendFolder = U.resolveIgniteUrl("frontend", false);

    /** Favicon resource url. */
    private URL faviconUrl = U.resolveIgniteUrl("frontend/favicon.ico", false);

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

        if (frontendFolder != null) {
            registry.addResourceHandler("/**")
                .addResourceLocations(frontendFolder.toExternalForm());
        }
        else {
            registry.addResourceHandler("/**")
                .addResourceLocations("file:frontend/");

            log.info("If you are running Web Console on-premise, please ensure that " +
                "folder with frontend resources is present in " + new File("frontend").getAbsolutePath());
        }
    }

    /**
     *
     */
    @Bean
    public WebMvcConfigurerAdapter forwardToIndex() {
        return new WebMvcConfigurerAdapter() {
            @Override public void addViewControllers(ViewControllerRegistry registry) {
                // Map "/"
                registry.addViewController("/").setViewName("forward:/index.html");

                // Single directory level - no need to exclude "api"
                registry.addViewController("/{x:[\\w\\-]+}").setViewName("forward:/index.html");

                // Multi-level directory path, need to exclude "api" on the first part of the path
                registry.addViewController("/{x:^(?!api$).*$}/**/{y:[\\w\\-]+}").setViewName("forward:/index.html");
            }
        };
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

        if (faviconUrl != null)
            reqHnd.setLocations(Collections.singletonList(applicationCtx.getResource(faviconUrl.toExternalForm())));
        else if (frontendFolder != null)
            log.warn("Favicon not found locally: " + new File("frontend/favicon.ico").getAbsolutePath());

        return reqHnd;
    }
}

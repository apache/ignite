/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.springframework.boot.autoconfigure;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Ignite client auto configuration.
 * Spring will use this class to autoconfigure Ignite client on application startup.
 *
 * Initialization of the node will be the following:
 * <ul>
 *   <li>If {@link ClientConfiguration} bean exists in {@link BeanFactory} it will be used for the node start.</li>
 *   <li>If {@link ClientConfiguration} bean doesn't exists following rules are applied:
 *     <ol>
 *       <li>If bean of the type {@link IgniteClientConfigurer} exists in {@link BeanFactory}
 *         it will be used to customize empty {@link ClientConfiguration} instance.
 *         If you want to set custom SPI instances or similar hardcoded values.
 *         You should do it in custom {@link IgniteClientConfigurer}.</li>
 *       <li>Application properties will override config values. Prefix for properties names is "ignite-client".</li>
 *     </ol>
 *   </li>
 * </ul>
 *
 * Example of application property:
 * <pre>
 *   ignite-client.clientAddresses=127.0.0.1:10800
 * </pre>
 *
 * @see IgniteClient
 * @see ClientConfiguration
 */
@Configuration
@EnableConfigurationProperties
public class IgniteClientAutoConfiguration {
    /** Ignite client configuration properties prefix. */
    public static final String IGNITE_CLIENT_PROPS_PREFIX = "ignite-client";

    /**
     * @return Default(empty) Ignite client configurer.
     */
    @ConditionalOnMissingBean
    @Bean
    public IgniteClientConfigurer clientConfigurer() {
        return cfg -> { /* No-op. */ };
    }

    /**
     * Provides an instance of {@link ClientConfiguration} customized with the {@link IgniteClientConfigurer}.
     *
     * @param configurer Configurer.
     * @return Client configuration.
     */
    @ConditionalOnMissingBean
    @Bean
    @ConfigurationProperties(prefix = IGNITE_CLIENT_PROPS_PREFIX)
    public ClientConfiguration igniteCfg(IgniteClientConfigurer configurer) {
        ClientConfiguration cfg = new ClientConfiguration();

        configurer.accept(cfg);

        return cfg;
    }

    /**
     * Starts an {@link IgniteClient}.
     *
     * @param cfg Configuration.
     * @return Client instance.
     */
    @ConditionalOnBean(ClientConfiguration.class)
    @Bean
    public IgniteClient igniteClient(ClientConfiguration cfg) {
        return Ignition.startClient(cfg);
    }
}

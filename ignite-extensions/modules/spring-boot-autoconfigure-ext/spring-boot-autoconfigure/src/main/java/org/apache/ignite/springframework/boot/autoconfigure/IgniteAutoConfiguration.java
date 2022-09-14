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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Ignite node auto configuration.
 * Spring will use this class to autoconfigure Ignite node on application startup.
 *
 * Initialization of the node will be the following:
 * <ul>
 *   <li>If {@link IgniteConfiguration} bean exists in {@link BeanFactory} it will be used for the node start.</li>
 *   <li>If {@link IgniteConfiguration} bean doesn't exists following rules are applied:
 *     <ol>
 *       <li>If bean of the type {@link IgniteConfigurer} exists in {@link BeanFactory}
 *         it will be used to customize empty {@link IgniteConfiguration} instance.
 *         If you want to set custom SPI instances or similar hardcoded values.
 *         You should do it in custom {@link IgniteConfigurer}.</li>
 *       <li>Application properties will override config values. Prefix for properties names is "ignite".</li>
 *     </ol>
 *   </li>
 * </ul>
 *
 * Example of application property:
 * <pre>
 *   ignite.igniteInstanceName=my-instance
 * </pre>
 *
 * @see IgniteConfigurer
 * @see IgniteConfiguration
 * @see Ignite
 */
@Configuration
@EnableConfigurationProperties
public class IgniteAutoConfiguration {
    /** Ignite configuration properties prefix. */
    public static final String IGNITE_PROPS_PREFIX = "ignite";

    /**
     * @return Default(empty) Ignite configurer.
     */
    @ConditionalOnMissingBean
    @Bean
    public IgniteConfigurer nodeConfigurer() {
        return cfg -> { /* No-op. */ };
    }

    /**
     * Provides an instance of {@link IgniteConfiguration} customized with the {@link IgniteConfigurer}.
     *
     * @param configurer Configurer.
     * @return Ignite configuration.
     */
    @ConditionalOnMissingBean
    @Bean
    @ConfigurationProperties(prefix = IGNITE_PROPS_PREFIX)
    public IgniteConfiguration igniteConfiguration(IgniteConfigurer configurer) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        configurer.accept(cfg);

        return cfg;
    }

    /**
     * Starts an {@link Ignite} node.
     *
     * @param cfg Configuration.
     * @return Ignite instance.
     */
    @ConditionalOnBean(IgniteConfiguration.class)
    @Bean
    public Ignite igniteInstance(IgniteConfiguration cfg) {
        return Ignition.start(cfg);
    }
}

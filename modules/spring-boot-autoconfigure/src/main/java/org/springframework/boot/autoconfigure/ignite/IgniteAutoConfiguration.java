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

package org.springframework.boot.autoconfigure.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration class.
 */
@Configuration
@EnableConfigurationProperties(IgniteProperties.class)
public class IgniteAutoConfiguration {
    /** Properties prefix. */
    public static final String IGNITE_PROPS_PREFIX = "spring.data.ignite";

    @ConditionalOnBean(IgniteConfiguration.class)
    @Bean
    public Ignite ignite(IgniteConfiguration cfg) {
        return Ignition.start(cfg);
    }

    @ConditionalOnProperty(prefix = IGNITE_PROPS_PREFIX, name = "clientAddresses")
    @Bean
    public ClientConfiguration clientConfiguration(IgniteProperties properties) {
        if (properties.getClientAddresses() == null || properties.getClientAddresses().length == 0)
            return null;

        return new ClientConfiguration().setAddresses(properties.getClientAddresses());
    }

    @ConditionalOnBean(ClientConfiguration.class)
    @Bean
    public IgniteClient igniteClient(ClientConfiguration config) {
        return Ignition.startClient(config);
    }
}

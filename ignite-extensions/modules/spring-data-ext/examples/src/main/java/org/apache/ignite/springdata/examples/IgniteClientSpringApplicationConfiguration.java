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

package org.apache.ignite.springdata.examples;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springdata.repository.config.RepositoryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Example of Spring application configuration that represents beans required to configure Spring Data repository access
 * to an Ignite cluster through the thin client.
 *
 * Note that both Ignite thin client and Ignite node approaches of Ignite cluster access configuration uses the same API.
 * Ignite Spring Data integration automatically recognizes the type of provided bean and use the appropriate
 * cluster connection.
 *
 * @see SpringApplicationConfiguration
 */
@Configuration
@EnableIgniteRepositories
public class IgniteClientSpringApplicationConfiguration {
    /**
     * Creates Apache Ignite thin client instance bean which will be used for accessing the Ignite cluster.
     * Note, that the name of the current bean must match value of {@link RepositoryConfig#igniteInstance}
     * property that {@link PersonRepository} is marked with. In this particular case, the default value of
     * {@link RepositoryConfig#igniteInstance} property is used.
     *
     * @return Ignite Client.
     */
    @Bean
    public IgniteClient igniteInstance() {
        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
    }
}

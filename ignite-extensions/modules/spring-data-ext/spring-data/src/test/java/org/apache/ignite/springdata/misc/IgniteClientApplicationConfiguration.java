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

package org.apache.ignite.springdata.misc;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.springdata.misc.SampleEvaluationContextExtension.SamplePassParamExtension;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.spel.spi.EvaluationContextExtension;

import static org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration.CLI_CONN_PORT;
import static org.apache.ignite.springdata.misc.ApplicationConfiguration.IGNITE_INSTANCE_ONE;
import static org.apache.ignite.springdata.misc.ApplicationConfiguration.IGNITE_INSTANCE_TWO;

/** Spring Application configuration for repository testing in case thin client is used for accessing the cluster. */
@Configuration
@EnableIgniteRepositories({"org.apache.ignite.springdata.compoundkey", "org.apache.ignite.springdata.misc"})
public class IgniteClientApplicationConfiguration {
    /** Test cache name. */
    public static final String CACHE_NAME = "PersonCache";

    /** */
    @Bean
    public CacheNamesBean cacheNames() {
        CacheNamesBean bean = new CacheNamesBean();

        bean.setPersonCacheName(CACHE_NAME);

        return bean;
    }

    /** */
    @Bean
    public EvaluationContextExtension sampleSpELExtension() {
        return new SampleEvaluationContextExtension();
    }

    /** */
    @Bean(value = "sampleExtensionBean")
    public SamplePassParamExtension sampleExtensionBean() {
        return new SamplePassParamExtension();
    }

    /** */
    @Bean
    public Ignite igniteServerNode() {
        return Ignition.start(igniteConfiguration(IGNITE_INSTANCE_ONE, CLI_CONN_PORT));
    }

    /** */
    @Bean
    public Ignite igniteSecondServerNode() {
        return Ignition.start(igniteConfiguration(IGNITE_INSTANCE_TWO, 10801));
    }

    /** Ignite client instance bean with default name. */
    @Bean
    public IgniteClient igniteInstance() {
        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + CLI_CONN_PORT)
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false)));
    }

    /** Ignite client instance bean with non-default name. */
    @Bean
    public IgniteClient igniteInstanceTWO() {
        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10801"));
    }

    /** */
    private static IgniteConfiguration igniteConfiguration(String igniteInstanceName, int cliConnPort) {
        return new IgniteConfiguration()
            .setIgniteInstanceName(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true)))
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME).setIndexedTypes(Integer.class, Person.class))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(cliConnPort))
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));
    }
}

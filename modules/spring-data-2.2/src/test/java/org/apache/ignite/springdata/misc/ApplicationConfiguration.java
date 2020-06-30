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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.springdata.misc.SampleEvaluationContextExtension.SamplePassParamExtension;
import org.apache.ignite.springdata22.repository.config.EnableIgniteRepositories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.spel.spi.EvaluationContextExtension;

/** */
@Configuration
@EnableIgniteRepositories
public class ApplicationConfiguration {
    /** */
    public static final String IGNITE_INSTANCE_ONE = "IGNITE_INSTANCE_ONE";

    /** */
    public static final String IGNITE_INSTANCE_TWO = "IGNITE_INSTANCE_TWO";

    /**
     * The bean with cache names
     */
    @Bean
    public CacheNamesBean cacheNames() {
        CacheNamesBean bean = new CacheNamesBean();

        bean.setPersonCacheName("PersonCache");

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

    /**
     * Ignite instance bean - no instance name provided on RepositoryConfig
     */
    @Bean
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(IGNITE_INSTANCE_ONE);

        CacheConfiguration ccfg = new CacheConfiguration("PersonCache");

        ccfg.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(spi);

        return Ignition.start(cfg);
    }

    /**
     * Ignite instance bean with not default name
     */
    @Bean
    public Ignite igniteInstanceTWO() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(IGNITE_INSTANCE_TWO);

        CacheConfiguration ccfg = new CacheConfiguration("PersonCache");

        ccfg.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(spi);

        return Ignition.start(cfg);
    }
}

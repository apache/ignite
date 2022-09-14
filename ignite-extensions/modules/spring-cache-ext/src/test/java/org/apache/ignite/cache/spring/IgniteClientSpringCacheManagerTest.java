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

package org.apache.ignite.cache.spring;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/** Tests Spring Cache manager implementation that uses thin client to connect to the Ignite cluster. */
public class IgniteClientSpringCacheManagerTest extends GridSpringCacheManagerAbstractTest {
    /** */
    private AbstractApplicationContext ctx;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));
        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ctx = new AnnotationConfigApplicationContext(ClientInstanceApplicationContext.class);

        svc = ctx.getBean(GridSpringCacheTestService.class);
        dynamicSvc = ctx.getBean(GridSpringDynamicCacheTestService.class);

        svc.reset();
        dynamicSvc.reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        grid().cache(CACHE_NAME).removeAll();

        grid().destroyCache(DYNAMIC_CACHE_NAME);

        ctx.stop();
    }

    /**
     * Tests that {@link IgniteClientSpringCacheManager} successfully creates {@link IgniteClient} instance with
     * provided {@link ClientConfiguration}.
     */
    @Test
    public void testClientConfiguration() {
        try (
            AbstractApplicationContext ctx = new AnnotationConfigApplicationContext(
                ClientConfigurationApplicationContext.class)
        ) {
            IgniteClient cli = ctx.getBean(IgniteClientSpringCacheManager.class).getClientInstance();

            assertNotNull(cli);
            assertEquals(1, cli.cluster().nodes().size());
        }
    }

    /** Tests {@link IgniteClientSpringCacheManager} behaviour in case no connection configuration is specified. */
    @Test
    @SuppressWarnings("EmptyTryBlock")
    public void testOmittedConnectionConfiguration() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                try (
                    AbstractApplicationContext ignored = new AnnotationConfigApplicationContext(
                        InvalidConnectionConfigurationApplicationContext.class)
                ) {
                    // No-op.
                }

                return null;
            },
            IllegalArgumentException.class,
            "Neither client instance nor client configuration is specified.");
    }

    /** Tests {@link IgniteClientSpringCacheManager} configuration approach through XML file. */
    @Test
    public void testCacheManagerXmlConfiguration() {
        try (
            AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
                "org/apache/ignite/cache/spring/ignite-client-spring-caching.xml")
        ) {
            IgniteClientSpringCacheManager mgr = ctx.getBean(IgniteClientSpringCacheManager.class);

            assertNotNull(mgr.getClientConfiguration());

            IgniteClient cli = mgr.getClientInstance();

            assertNotNull(cli);
            assertEquals(2, cli.cluster().nodes().size());

            ClientCacheConfiguration cfg = mgr.getDynamicCacheConfiguration();

            assertEquals(2, cfg.getBackups());
        }
    }

    /** Test {@link Cacheable} annotation with {@code sync} mode enabled. */
    @Test
    public void testSyncMode() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                dynamicSvc.cacheableSync(0);

                return null;
            },
            UnsupportedOperationException.class,
            "Synchronous mode is not supported for the Ignite Spring Cache implementation that uses a thin client" +
                " to connecting to an Ignite cluster."
        );
    }

    /** */
    @Configuration
    @EnableCaching
    public static class ClientInstanceApplicationContext extends CachingConfigurerSupport {
        /** */
        @Bean
        public GridSpringCacheTestService cacheService() {
            return new GridSpringCacheTestService();
        }

        /** */
        @Bean
        public GridSpringDynamicCacheTestService dynamicCacheService() {
            return new GridSpringDynamicCacheTestService();
        }

        /** */
        @Bean
        public IgniteClient igniteClient() {
            return Ignition.startClient(new ClientConfiguration()
                .setAddresses("127.0.0.1:" + DFLT_PORT)
                .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)));
        }

        /** */
        @Bean
        public AbstractCacheManager cacheManager(IgniteClient cli) {
            return new IgniteClientSpringCacheManager()
                .setClientInstance(cli)
                .setDynamicCacheConfiguration(new ClientCacheConfiguration().setBackups(2));
        }

        /** {@inheritDoc} */
        @Override public KeyGenerator keyGenerator() {
            return new GridSpringCacheTestKeyGenerator();
        }
    }

    /** */
    @Configuration
    @EnableCaching
    public static class ClientConfigurationApplicationContext {
        /** */
        @Bean
        public AbstractCacheManager cacheManager() {
            return new IgniteClientSpringCacheManager()
                .setClientConfiguration(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
        }
    }

    /** */
    @Configuration
    @EnableCaching
    public static class InvalidConnectionConfigurationApplicationContext {
        /** */
        @Bean
        public AbstractCacheManager cacheManager() {
            return new IgniteClientSpringCacheManager();
        }
    }
}

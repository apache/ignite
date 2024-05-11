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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_11_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.since;
import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class IgniteSpringCacheCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** */
    private static final Collection<String> TEST_SPRING_VERSIONS = Arrays.asList(
        "4.3.0.RELEASE",
        "5.0.0.RELEASE",
        "5.1.0.RELEASE",
        "5.2.0.RELEASE",
        "5.3.0"
    );

    /** */
    private static final String TEST_CACHE_NAME = "testCache";

    /** */
    @Parameterized.Parameters(name = "Ignite Version {0}, Spring Version {1}")
    public static Iterable<Object[]> versions() {
        return cartesianProduct(F.concat(true, VER_STR, since(VER_2_11_0)), TEST_SPRING_VERSIONS);
    }

    /** */
    @Parameterized.Parameter
    public String igniteVer;

    /** */
    @Parameterized.Parameter(1)
    public String springVer;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String ver) {
        if (!F.isEmpty(ver))
            return super.getDependencies(ver);

        Collection<Dependency> res = new ArrayList<>();

        res.add(new Dependency("core", "org.apache.ignite", "ignite-core", igniteVer, false));
        res.add(new Dependency("core", "org.apache.ignite", "ignite-core", igniteVer, true));
        res.add(new Dependency("spring", "org.apache.ignite", "ignite-spring", igniteVer, false));

        res.add(new Dependency("spring-context", "org.springframework", "spring-context", springVer, false));
        res.add(new Dependency("spring-beans", "org.springframework", "spring-beans", springVer, false));
        res.add(new Dependency("spring-core", "org.springframework", "spring-core", springVer, false));
        res.add(new Dependency("spring-aop", "org.springframework", "spring-aop", springVer, false));
        res.add(new Dependency("spring-expression", "org.springframework", "spring-expression", springVer, false));

        if (VER_2_11_0.toString().equals(igniteVer)) {
            res.add(new Dependency("spring-tx", "org.springframework", "spring-tx", springVer, false));
            res.add(new Dependency("spring-jdbc", "org.springframework", "spring-jdbc", springVer, false));
        }

        if (IgniteProductVersion.fromString("2.14.0").compareTo(IgniteProductVersion.fromString(igniteVer)) > 0)
            res.add(new Dependency("log4j", "log4j", "log4j", "1.2.17", false));

        return res;
    }

    /** */
    protected void processNodeConfiguration(IgniteConfiguration cfg) {
        cfg.setCacheConfiguration(new CacheConfiguration<>(TEST_CACHE_NAME))
            .setBinaryConfiguration(new BinaryConfiguration()
                .setCompactFooter(true));
    }

    /** */
    @Test
    public void testCompatibility() throws Exception {
        startGrid(1, igniteVer, this::processNodeConfiguration, ignite -> {});

        GridJavaProcess proc = GridJavaProcess.exec(
            TestRunner.class.getName(),
            "",
            log,
            log::info,
            null,
            null,
            getProcessProxyJvmArgs(""),
            null
        );

        try {
            assertTrue(waitForCondition(() -> !proc.getProcess().isAlive(), getTestTimeout()));

            assertEquals(0, proc.getProcess().exitValue());
        }
        finally {
            if (proc.getProcess().isAlive())
                proc.kill();
        }
    }

    /** */
    public static class TestRunner {
        /** */
        private static void doIgniteClientConnectionTest() {
            try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
                ctx.register(ClientInstanceApplicationContext.class);
                ctx.refresh();

                GridSpringCacheTestService svc = ctx.getBean(GridSpringCacheTestService.class);

                IgniteClient cli = ctx.getBean(IgniteClient.class);

                ClientCache<Integer, String> cache = cli.cache(TEST_CACHE_NAME);

                cache.clear();
                svc.reset();

                for (int i = 0; i < 3; i++) {
                    assertEquals("value" + i, svc.simpleKey(i));
                    assertEquals("value" + i, svc.simpleKey(i));
                }

                assertEquals(3, svc.called());
                assertEquals(3, cache.size());

                for (int i = 0; i < 3; i++)
                    assertEquals("value" + i, cache.get(i));
            }
        }

        /** */
        private static void doIgniteNodeConnectionTest() {
            try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
                ctx.register(IgniteNodeApplicationContext.class);
                ctx.refresh();

                GridSpringCacheTestService svc = ctx.getBean(GridSpringCacheTestService.class);

                Ignite cli = ctx.getBean(Ignite.class);

                IgniteCache<Integer, String> cache = cli.cache(TEST_CACHE_NAME);

                cache.clear();
                svc.reset();

                for (int i = 0; i < 3; i++) {
                    assertEquals("value" + i, svc.simpleKey(i));
                    assertEquals("value" + i, svc.simpleKey(i));
                }

                assertEquals(3, svc.called());
                assertEquals(3, cache.size());

                for (int i = 0; i < 3; i++)
                    assertEquals("value" + i, cache.get(i));
            }
        }

        /** */
        public static void main(String[] args) {
            doIgniteNodeConnectionTest();

            doIgniteClientConnectionTest();
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
            public IgniteClient igniteClient() {
                return Ignition.startClient(new ClientConfiguration()
                    .setAddresses("127.0.0.1:" + DFLT_PORT)
                    .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)));
            }

            /** */
            @Bean
            public AbstractCacheManager cacheManager(IgniteClient cli) {
                return new IgniteClientSpringCacheManager().setClientInstance(cli);
            }

            /** {@inheritDoc} */
            @Override public KeyGenerator keyGenerator() {
                return new GridSpringCacheTestKeyGenerator();
            }
        }

        /** */
        @Configuration
        @EnableCaching
        public static class IgniteNodeApplicationContext extends CachingConfigurerSupport {
            /** */
            @Bean
            public GridSpringCacheTestService cacheService() {
                return new GridSpringCacheTestService();
            }

            /** */
            @Bean
            public Ignite igniteInstance() {
                return Ignition.start("config/ignite-client-node.xml");
            }

            /** */
            @Bean
            @Override public AbstractCacheManager cacheManager() {
                SpringCacheManager mgr = new SpringCacheManager();

                mgr.setIgniteInstanceName("ignite-client-node");

                return mgr;
            }

            /** {@inheritDoc} */
            @Override public KeyGenerator keyGenerator() {
                return new GridSpringCacheTestKeyGenerator();
            }
        }
    }
}


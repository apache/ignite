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
package org.apache.ignite.p2p;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class P2PScanQueryUndeployTest extends GridCommonAbstractTest {
    /** Class name of predicate. */
    private static final String TEST_PREDICATE_NAME = "org.apache.ignite.tests.p2p.cache.TestScanQueryPredicate";

    private static final String TEST_PREDICATE_CLASS_NAME = TEST_PREDICATE_NAME.substring(TEST_PREDICATE_NAME.lastIndexOf('.') + 1);

    private static final String TEST_STATIC_PREDICATE_NAME = "org.apache.ignite.tests.p2p.cache.TestScanStaticQueryPredicate";

    private static final String TEST_STATIC_PREDICATE_CLASS_NAME = TEST_STATIC_PREDICATE_NAME.substring(TEST_STATIC_PREDICATE_NAME.lastIndexOf('.') + 1);

    private static final String FIELD_NAME = "VALUE";

    private static final String VALUE_1 = "foo";
    private static final String VALUE_2 = "bar";

    /**
     * URL of classes.
     */
    private static final URL[] URLS;

    /** Cache name. */
    private static final String CACHE_NAME = "foo";

    /** Client instance name. */
    private static final String CLIENT_INSTANCE_NAME = "client";

    /**
     * Initialize URLs.
     */
    static {
        try {
            URLS = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setCacheConfiguration(
            new CacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
        );

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder(true)
                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                )
        );

        cfg.setPeerClassLoadingLocalClassPathExclude(TestPredicate.class.getName(), TEST_PREDICATE_CLASS_NAME, TEST_STATIC_PREDICATE_CLASS_NAME);

        if (igniteInstanceName.equals(CLIENT_INSTANCE_NAME))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return super.isRemoteJvm(igniteInstanceName) && !igniteInstanceName.equals(CLIENT_INSTANCE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    public void testStaticScanQueryUndeployedOnClientDisconnect() throws Exception {
        doTest(TEST_STATIC_PREDICATE_NAME, true);
    }

    public void testScanQueryUndeployedOnClientDisconnect() throws Exception {
        doTest(TEST_PREDICATE_NAME, false);
    }

    private void doTest(String className, boolean isStatic) throws Exception {
        startGrids(2);

        Ignite client = startGrid(CLIENT_INSTANCE_NAME);

        stopGrid(0);

        client.cluster().active(true);

        ClassLoader ldr = new URLClassLoader(URLS, getClass().getClassLoader());

        Class<?> predicateClass = ldr.loadClass(className);

        createPredicateAndExecuteQuery(predicateClass, VALUE_1, client, isStatic);

        stopGrid(CLIENT_INSTANCE_NAME);

        client = startGrid(CLIENT_INSTANCE_NAME);

        createPredicateAndExecuteQuery(predicateClass, VALUE_2, client, isStatic);
    }

    private void createPredicateAndExecuteQuery(
        Class<?> predicateClass,
        String value,
        Ignite client,
        boolean isStatic
    ) throws Exception {
        Field field = predicateClass.getDeclaredField(FIELD_NAME);

        field.setAccessible(true);

        IgniteBiPredicate<Integer, String> predicate;

        if (isStatic) {
            field.set(null, value);

            assertEquals(value,field.get(null));

            predicate = (IgniteBiPredicate<Integer, String>)predicateClass.newInstance();
        }
        else {
            predicate = (IgniteBiPredicate<Integer, String>)predicateClass.newInstance();

            field.set(predicate, value);

            assertEquals(value,field.get(predicate));
        }

        IgniteCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, value);

        assertEquals(1, cache.query(new ScanQuery(predicate)).getAll().size());
    }

    /**
     * Checks that scan query will be undeployed after client disconnect.
     *
     * @throws Exception if failed.
     */
    public void testOnClientDisconnect() throws Exception {
        startGrids(2);

        Ignite client = startGrid(CLIENT_INSTANCE_NAME);

        stopGrid(0);

        client.cluster().active(true);

        assertTrue(
            TestPredicate.class + " must be excluded from local class loading!",
            Arrays.asList(
                grid(1)
                    .configuration()
                    .getPeerClassLoadingLocalClassPathExclude()
            ).contains(TestPredicate.class.getName())
        );

        Set<String> cachedClassesBefore = client.compute(client.cluster().forRemotes()).call(new GetClassCacheTask());

        assertFalse(
            TestPredicate.class.getCanonicalName() + " can't be cached on remote node on remote jvm!",
            cachedClassesBefore.contains(TestPredicate.class.getName())
        );

        IgniteCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, "1");

        cache.query(new ScanQuery(new TestPredicate())).getAll();

        Set<String> cachedClassesAfterScanQry = client.compute(client.cluster().forRemotes()).call(new GetClassCacheTask());

        assertTrue(
            TestPredicate.class.getCanonicalName() + " must be cached on remote jvm!",
            cachedClassesAfterScanQry.contains(TestPredicate.class.getName())
        );

        stopGrid(CLIENT_INSTANCE_NAME);

        // Need's for checking, that TestPredicate was removed from node on remote jvm.
        startGrid(0);

        Set<String> cachedClassesAfterStopClient = grid(0).compute(grid(0).cluster().forRemotes()).call(new GetClassCacheTask());

        assertFalse(
            TestPredicate.class.getCanonicalName() + " must be removed from cache on remote node on remote jvm after disconnecting client!",
            cachedClassesAfterStopClient.contains(TestPredicate.class.getName())
        );
    }

    /** */
    private static class TestPredicate implements IgniteBiPredicate<Integer, String> {
        /** {@inheritDoc} */
        @Override public boolean apply(Integer integer, String s) {
            return true;
        }
    }

    /** */
    private static class GetClassCacheTask implements IgniteCallable<Set<String>> {
        /** {@inheritDoc} */
        @Override public Set<String> call() throws Exception {
            Field clsCacheField = IgniteUtils.class.getDeclaredField("classCache");

            clsCacheField.setAccessible(true);

            return ((ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>>)clsCacheField.get(null))
                .values()
                .stream()
                .flatMap(m -> m.keySet().stream())
                .collect(Collectors.toSet());
        }
    }

}
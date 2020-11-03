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

import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridP2PScanQueryWithTransformerTest extends GridCommonAbstractTest {
    /** Test class loader. */
    private static final ClassLoader TEST_CLASS_LOADER;

    /** Name of explicit class used as a Transformer for Scan Query. */
    private static final String TRANSFORMER_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformer";

    /** Name of class-wrapper for anonymous class used as a Transformer for Scan Query. */
    private static final String TRANSFORMER_CLO_WRAPPER_CLASS_NAME =
        "org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformerWrapper";

    /** */
    private static final int SCALE_FACTOR = 7;

    /** */
    private static final int CACHE_SIZE = 10;

    /** Initialize ClassLoader. */
    static {
        try {
            TEST_CLASS_LOADER = new URLClassLoader(
                new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                GridP2PScanQueryWithTransformerTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** */
    private boolean p2pEnabled;

    /** */
    private ClassLoader clsLoader;

    /** */
    private IgniteLogger logger;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(p2pEnabled);
        cfg.setClassLoader(clsLoader);
        if (logger != null)
            cfg.setGridLogger(logger);

        return cfg;
    }

    /**
     * Verifies that Scan Query Transformer is loaded by p2p mechanism <b>from client node</b>
     * when it is missing on server's classpath.
     *
     * Scan Query result set is examined by iteration over Cursor.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryCursorFromClientNodeWithExplicitClass() throws Exception {
        p2pEnabled = true;

        executeP2PClassLoadingEnabledTest(true);
    }

    /**
     * Verifies that Scan Query Transformer is loaded by p2p mechanism <b>from another server node</b>
     * when it is missing on server's classpath.
     *
     * Scan Query result set is examined by iteration over Cursor.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryCursorFromServerNodeWithExplicitClass() throws Exception {
        p2pEnabled = true;

        executeP2PClassLoadingEnabledTest(false);
    }

    /**
     * Verifies that Scan Query Transformer is loaded by p2p mechanism <b>from client node</b>
     * when it is missing on server's classpath.
     *
     * Scan Query result set is examined by getAll call from Cursor..
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryGetAllFromClientNodeWithExplicitClass() throws Exception {
        p2pEnabled = true;

        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClass());

        List<Integer> results = query.getAll();

        assertNotNull(results);
        assertEquals(CACHE_SIZE, results.size());
    }

    /**
     * Verifies that Scan Query Transformer is loaded by p2p mechanism <b>from client node</b>
     * when it is missing on server's classpath.
     *
     * Transformer class is implemented as an anonymous instance of {@link IgniteClosure} interface.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryCursorFromClientNodeWithAnonymousClass() throws Exception {
        p2pEnabled = true;

        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        int sumPopulated = populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClosure());

        int sumQueried = 0;

        for (Integer val : query)
            sumQueried += val;

        assertTrue(sumQueried == sumPopulated * SCALE_FACTOR);
    }

    /**
     * Verifies that execution <b>from client node</b> of Scan Query with Transformer
     * fails if Transformer class is missing on server node and p2p class loading is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryFromClientFailsIfP2PClassLoadingIsDisabled() throws Exception {
        p2pEnabled = false;

        executeP2PClassLoadingDisabledTest(true);
    }

    /**
     * Verifies that execution <b>from server node</b> of Scan Query with Transformer
     * fails if Transformer class is missing on server node and p2p class loading is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryFromServerFailsIfP2PClassLoadingIsDisabled() throws Exception {
        p2pEnabled = false;

        executeP2PClassLoadingDisabledTest(false);
    }

    /**
     * Verifies that deployment isn't needed if Transformer class is available on classpath of all nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSharedTransformerWorksWhenP2PIsDisabled() throws Exception {
        p2pEnabled = false;

        IgniteEx ig0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        populateCache(cache);

        IgniteEx client = startClientGrid(1);

        IgniteCache<Object, Object> clientCache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = clientCache.query(new ScanQuery<Integer, Integer>(),
            new SharedTransformer(SCALE_FACTOR));

        List<Integer> results = query.getAll();

        assertNotNull(results);
        assertEquals(CACHE_SIZE, results.size());
    }

    /**
     * Executes scenario with successful p2p loading of Transformer class
     * with client or server node sending Scan Query request and iterating over result set.
     *
     * @param withClientNode Flag to execute scan query from client or server node.
     * @throws Exception If failed.
     */
    private void executeP2PClassLoadingEnabledTest(boolean withClientNode) throws Exception {
        ListeningTestLogger listeningLogger = new ListeningTestLogger();
        LogListener clsDeployedMsgLsnr = LogListener.matches(
            "Class was deployed in SHARED or CONTINUOUS mode: " +
                "class org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformer")
            .build();
        listeningLogger.registerListener(clsDeployedMsgLsnr);
        logger = listeningLogger;

        IgniteEx ig0 = startGrid(0);

        logger = null;

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        int sumPopulated = populateCache(cache);

        IgniteEx requestingNode;

        if (withClientNode)
             requestingNode = startClientGrid(1);
        else {
            clsLoader = TEST_CLASS_LOADER;
            requestingNode = startGrid(1);
        }

        IgniteCache<Object, Object> reqNodeCache = requestingNode.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = reqNodeCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClass());

        int sumQueried = 0;

        for (Integer val : query)
            sumQueried += val;

        assertTrue(sumQueried == sumPopulated * SCALE_FACTOR);
        assertTrue(clsDeployedMsgLsnr.check());
    }

    /**
     * Executes scenario with p2p loading of Transformer class failed
     * with client or server node sending Scan Query request.
     *
     * @param withClientNode Flag to execute scan query from client or server node.
     * @throws Exception If test scenario failed.
     */
    private void executeP2PClassLoadingDisabledTest(boolean withClientNode) throws Exception {
        ListeningTestLogger listeningLogger = new ListeningTestLogger();
        LogListener clsDeployedMsgLsnr = LogListener.matches(
            "Class was deployed in SHARED or CONTINUOUS mode: " +
                "class org.apache.ignite.tests.p2p.cache.ScanQueryTestTransformerWrapper")
            .build();
        listeningLogger.registerListener(clsDeployedMsgLsnr);
        logger = listeningLogger;

        IgniteEx ig0 = startGrid(0);

        logger = null;

        IgniteCache<Integer, Integer> cache = ig0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        populateCache(cache);

        IgniteEx requestNode;

        if (withClientNode)
            requestNode = startClientGrid(1);
        else {
            clsLoader = TEST_CLASS_LOADER;
            requestNode = startGrid(1);
        }

        IgniteCache<Object, Object> reqNodeCache = requestNode.getOrCreateCache(DEFAULT_CACHE_NAME);

        QueryCursor<Integer> query = reqNodeCache.query(new ScanQuery<Integer, Integer>(), loadTransformerClosure());

        try {
            List<Integer> all = query.getAll();
        }
        catch (Exception e) {
            //No-op.

            checkTopology(2);

            assertFalse(clsDeployedMsgLsnr.check());

            return;
        }

        fail("Expected exception on executing scan query hasn't been not thrown.");
    }

    /**
     * @param cache Cache to populate.
     */
    private int populateCache(IgniteCache cache) {
        int sum = 0;

        for (int i = 0; i < CACHE_SIZE; i++) {
            sum += i;

            cache.put(i, i);
        }

        return sum;
    }

    /**
     * Loads class for query transformer from another package so server doesn't have access to it.
     *
     * @return Instance of transformer class.
     * @throws Exception If load has failed.
     */
    private IgniteClosure loadTransformerClass() throws Exception {
        Constructor ctor = TEST_CLASS_LOADER.loadClass(TRANSFORMER_CLASS_NAME).getConstructor(int.class);

        return (IgniteClosure)ctor.newInstance(SCALE_FACTOR);
    }

    /**
     * Loads anonymous class for query transformer from another package
     * so executing server node doesn't have access to it.
     *
     * @return Instance of anonymous class implementing {@link IgniteClosure} to be used as a transformer.
     * @throws Exception If load has failed.
     */
    private IgniteClosure loadTransformerClosure() throws Exception {
        Constructor<?> ctor = TEST_CLASS_LOADER.loadClass(TRANSFORMER_CLO_WRAPPER_CLASS_NAME).getConstructor(int.class);

        Object wrapper = ctor.newInstance(SCALE_FACTOR);

        return GridTestUtils.getFieldValue(wrapper, "clo");
    }

    /** */
    private static final class SharedTransformer implements IgniteClosure<Cache.Entry<Integer, Integer>, Integer> {
        /** */
        private final int scaleFactor;

        /** */
        private SharedTransformer(int factor) {
            scaleFactor = factor;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            return entry.getValue() * scaleFactor;
        }
    }
}

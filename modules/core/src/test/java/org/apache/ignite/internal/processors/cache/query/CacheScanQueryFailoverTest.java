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

package org.apache.ignite.internal.processors.cache.query;

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * ScanQuery failover test. Tests scenario where user supplied closures throw unhandled errors.
 */
public class CacheScanQueryFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String LOCAL_CACHE_NAME = "local";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        if(igniteInstanceName.equals("client") || igniteInstanceName.equals("server"))
            return false;
        else
            return super.isRemoteJvm(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (name.equals("client"))
            cfg.setClientMode(true);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryWithFailedClosures() throws Exception {
        Ignite srv = startGrids(4);
        Ignite client = startGrid("client");

        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME).setCacheMode(PARTITIONED);

        // Test query from client node.
        queryCachesWithFailedPredicates(client, cfg);

        // Test query from server node.
        queryCachesWithFailedPredicates(srv, cfg);

        assertEquals(client.cluster().nodes().size(), 5);
    };

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryOverLocalCacheWithFailedClosures() throws Exception {
        Ignite srv = startGrids(4);

        queryCachesWithFailedPredicates(srv, new CacheConfiguration(LOCAL_CACHE_NAME).setCacheMode(LOCAL));

        assertEquals(srv.cluster().nodes().size(), 4);
    };

    /**
     * @param ignite Ignite instance.
     * @param configs Cache configurations.
     */
    private void queryCachesWithFailedPredicates(Ignite ignite, CacheConfiguration... configs) {
        if (configs == null)
            return;

        for (CacheConfiguration cfg: configs) {
            IgniteCache cache = ignite.getOrCreateCache(cfg);

            populateCache(ignite, cache.getName());

            // Check that exception propagates to client from filter failure.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor =
                             cache.withKeepBinary().query(new ScanQuery<>(filter))) {
                    for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                        log.info("Entry " + entry.toString());
                }

                return null;
            }, Error.class, "Poison pill");

            // Check that exception propagates to client from transformer failure.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor =
                             cache.withKeepBinary().query(new ScanQuery<>(), transformer)) {
                    for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                        log.info("Entry " + entry.toString());
                }

                return null;
            }, Error.class, "Poison pill");
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param cacheName Cache name.
     */
    private void populateCache(Ignite ignite, String cacheName) {
        IgniteBinary binary = ignite.binary();

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, binary.builder("type_name").setField("f_" + i, "v_" + i).build());
        }
    }

    /** Failed filter. */
    private static IgniteBiPredicate<Integer, BinaryObject> filter = (key, value) -> {
            throw new Error("Poison pill");
        };

    /** Failed entry transformer. */
    private static IgniteClosure<Cache.Entry<Integer, BinaryObject>, Cache.Entry<Integer, BinaryObject>> transformer =
            integerBinaryObjectEntry -> {
                throw new Error("Poison pill");
        };
}

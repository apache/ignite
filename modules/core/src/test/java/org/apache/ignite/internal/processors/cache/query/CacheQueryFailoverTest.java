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

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * ScanQuery failover test. Tests scenario where user supplied closures throw unhandled errors.
 */
public class CacheQueryFailoverTest extends GridCommonAbstractTest {
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
        if(igniteInstanceName.equals("client"))
            return false;
        else
            return super.isRemoteJvm(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (name.equals("client"))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setCacheMode(REPLICATED)
                .setAtomicityMode(ATOMIC));

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryWithFailedClosures() throws Exception {
        startGrids(4);

        Ignite client = startGrid("client");

        IgniteBinary binary = client.binary();

        try (IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, binary.builder("type_name").setField("f_" + i, "v_" + i).build());
        }

        final IgniteBiPredicate<Integer, BinaryObject> filter = (key, value) -> {
            throw new Error("Poison pill");
        };

        final IgniteClosure<Cache.Entry<Integer, BinaryObject>, Cache.Entry<Integer, BinaryObject>> transformer =
                integerBinaryObjectEntry -> {
                    throw new Error("Poison pill");
                };

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        // Check that exception propagates to client from filter failure.
        GridTestUtils.assertThrowsAnyCause(log, () -> {
            try(QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache.withKeepBinary().query(new ScanQuery<>(filter))) {
                for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                    log.info("Entry " + entry.toString());
            }

            return null;
        }, Error.class, "Poison pill");

        // Check that exception propagates to client from transformer failure.
        GridTestUtils.assertThrowsAnyCause(log, () -> {
            try(QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache.withKeepBinary().query(new ScanQuery<>(), transformer)) {
                for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                    log.info("Entry " + entry.toString());
            }

            return null;
        }, Error.class, "Poison pill");

        assertEquals(client.cluster().nodes().size(), 5);
    };
}

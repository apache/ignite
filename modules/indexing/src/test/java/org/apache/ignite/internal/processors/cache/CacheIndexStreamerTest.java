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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheIndexStreamerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamerAtomic() throws Exception {
        checkStreamer(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamerTx() throws Exception {
        checkStreamer(TRANSACTIONAL);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    public void checkStreamer(CacheAtomicityMode atomicityMode) throws Exception {
        final Ignite ignite = startGrid(0);

        final IgniteCache<Integer, String> cache = ignite.createCache(cacheConfiguration(atomicityMode));

        final AtomicBoolean stop = new AtomicBoolean();

        final int KEYS = 10_000;

        try {
            IgniteInternalFuture streamerFut = GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        try (IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
                            // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11793
                            streamer.allowOverwrite(atomicityMode == TRANSACTIONAL);

                            for (int i = 0; i < 1; i++)
                                streamer.addData(rnd.nextInt(KEYS), String.valueOf(i));
                        }
                    }

                    return null;
                }
            }, "streamer-thread");

            IgniteInternalFuture updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        for (int i = 0; i < 100; i++) {
                            Integer key = rnd.nextInt(KEYS);

                            cache.put(key, String.valueOf(key));

                            cache.remove(key);
                        }
                    }

                    return null;
                }
            }, 1, "update-thread");

            U.sleep(30_000);

            stop.set(true);

            streamerFut.get();
            updateFut.get();
        }
        finally {
            stop.set(true);

            stopAllGrids();
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);
        ccfg.setIndexedTypes(Integer.class, String.class);

        return ccfg;
    }
}

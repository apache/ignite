/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;

/**
 * Checks that top value at {@link GridCachePartitionExchangeManager#exchangeFutures()} is the newest one.
 */
public class IgniteExchangeFutureHistoryTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * Checks reverse order of exchangeFutures.
     *
     * @throws Exception If failed.
     */
    public void testExchangeFutures() throws Exception {
        GridCachePartitionExchangeManager mgr = ((IgniteKernal)grid(0)).internalCache().context().shared().exchange();

        for (int i = 1; i <= 10; i++) {
            startGrid(i);

            List<GridDhtPartitionsExchangeFuture> futs = mgr.exchangeFutures();

            List<GridDhtPartitionsExchangeFuture> sortedFuts = new ArrayList<>(futs);

            Collections.sort(sortedFuts, Collections.reverseOrder());

            for (int j = 0; j < futs.size(); j++)
                assertEquals(futs.get(j), sortedFuts.get(j));
        }
    }
}
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
 *
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicMultipleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicMultipleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import java.util.HashMap;
import java.util.Map;

public abstract class CacheAtomicSingleUpdateSelfTest extends GridCacheAbstractSelfTest {

    @Override protected int gridCount() {
        return 2;
    }

    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    protected abstract CacheAtomicWriteOrderMode atomicWriteOrderMode();

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals("client"))
            cfg.setClientMode(true);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.record(GridNearAtomicMultipleUpdateRequest.class);
        commSpi.record(GridNearAtomicSingleUpdateRequest.class);
        commSpi.record(GridDhtAtomicMultipleUpdateRequest.class);
        commSpi.record(GridDhtAtomicSingleUpdateRequest.class);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(atomicWriteOrderMode());

        return ccfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        // noop
    }

    @Override protected void afterTestsStopped() throws Exception {
        // noop
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTestsStarted();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTestsStopped();
    }

    public void testMultipleUpdates() throws Exception {
        Ignite client = startGrid("client", getConfiguration("client"));

        final IgniteCache<Integer, String> cache = grid(0).cache(null);

        Map<Integer, String> vals = new HashMap<>();
        vals.put(1, "1");
        vals.put(2, "2");

        client.cache(null).putAll(vals);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.containsKey(1) && cache.containsKey(2);
            }
        }, 5000);

        int singleUpdateRequests = 0;
        int multipleUpdateRequests = 0;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            for (Object msg : commSpi.recordedMessages()) {
                if (msg instanceof GridNearAtomicSingleUpdateRequest || msg instanceof GridDhtAtomicSingleUpdateRequest)
                    singleUpdateRequests++;
                else if (msg instanceof GridNearAtomicMultipleUpdateRequest || msg instanceof GridDhtAtomicMultipleUpdateRequest)
                    multipleUpdateRequests++;
            }
        }

        assert multipleUpdateRequests > 0;
        assert singleUpdateRequests == 0;
    }

    public void testSingleUpdate() throws Exception {
        Ignite client = startGrid("client", getConfiguration("client"));

        final IgniteCache<Integer, String> cache = grid(0).cache(null);

        client.cache(null).put(1, "1");

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.containsKey(1);
            }
        }, 5000);

        int singleUpdateRequests = 0;
        int multipleUpdateRequests = 0;

        for (Ignite ignite : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            for (Object msg : commSpi.recordedMessages()) {
                if (msg instanceof GridNearAtomicSingleUpdateRequest || msg instanceof GridDhtAtomicSingleUpdateRequest)
                    singleUpdateRequests++;
                else if (msg instanceof GridNearAtomicMultipleUpdateRequest || msg instanceof GridDhtAtomicMultipleUpdateRequest)
                    multipleUpdateRequests++;
            }
        }

        assert multipleUpdateRequests == 0;
        assert singleUpdateRequests > 0;
    }
}

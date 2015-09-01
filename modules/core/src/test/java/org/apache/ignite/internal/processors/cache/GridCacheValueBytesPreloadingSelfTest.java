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

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheValueBytesPreloadingSelfTest extends GridCommonAbstractTest {
    /** Memory mode. */
    private CacheMemoryMode memMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setNearConfiguration(null);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setMemoryMode(memMode);
        ccfg.setOffHeapMaxMemory(1024 * 1024 * 1024);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapTiered() throws Exception {
        memMode = ONHEAP_TIERED;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapTiered() throws Exception {
        memMode = OFFHEAP_TIERED;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapValuesOnly() throws Exception {
        memMode = OFFHEAP_VALUES;

        startGrids(1);

        try {
            checkByteArrays();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void checkByteArrays() throws Exception {
        int keyCnt = 1000;

        byte[] val = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(null).put(String.valueOf(i), val);

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(null).get(String.valueOf(i));

        startGrid(1);

        if (memMode == ONHEAP_TIERED) {
            for (int i = 0; i < keyCnt; i++)
                grid(0).cache(null).localEvict(Collections.<Object>singleton(String.valueOf(i)));

            for (int i = 0; i < keyCnt; i++)
                grid(0).cache(null).localPromote(Collections.singleton(String.valueOf(i)));
        }

        startGrid(2);

        for (int g = 0; g < 3; g++) {
            for (int i = 0; i < keyCnt; i++) {
                byte[] o = (byte[])grid(g).cache(null).get(String.valueOf(i));

                assertTrue("Got invalid value [val=" + Arrays.toString(val) + ", actual=" + Arrays.toString(o) + ']',
                    Arrays.equals(val, o));
            }
        }
    }
}
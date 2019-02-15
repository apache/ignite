/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCacheValueBytesPreloadingSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setNearConfiguration(null);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnHeapTiered() throws Exception {
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
    private void checkByteArrays() throws Exception {
        int keyCnt = 1000;

        byte[] val = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(DEFAULT_CACHE_NAME).put(String.valueOf(i), val);

        for (int i = 0; i < keyCnt; i++)
            grid(0).cache(DEFAULT_CACHE_NAME).get(String.valueOf(i));

        startGrid(1);

// TODO: GG-11148 check if evict/promote make sense.
//        if (memMode == ONHEAP_TIERED) {
//            for (int i = 0; i < keyCnt; i++)
//                grid(0).cache(DEFAULT_CACHE_NAME).localEvict(Collections.<Object>singleton(String.valueOf(i)));
//
//            for (int i = 0; i < keyCnt; i++)
//                grid(0).cache(DEFAULT_CACHE_NAME).localPromote(Collections.singleton(String.valueOf(i)));
//        }

        startGrid(2);

        for (int g = 0; g < 3; g++) {
            for (int i = 0; i < keyCnt; i++) {
                byte[] o = (byte[])grid(g).cache(DEFAULT_CACHE_NAME).get(String.valueOf(i));

                assertTrue("Got invalid value [val=" + Arrays.toString(val) + ", actual=" + Arrays.toString(o) + ']',
                    Arrays.equals(val, o));
            }
        }
    }
}

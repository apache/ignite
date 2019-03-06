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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Checkpoint triggered WAL delta records consistency test.
 */
public class CpTriggeredWalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected boolean checkPagesOnCheckpoint() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testPutRemoveCacheDestroy() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache0 = ignite.createCache(cacheConfiguration("cache0"));

        for (int i = 0; i < 3_000; i++)
            cache0.put(i, "Cache value " + i);

        for (int i = 2_000; i < 5_000; i++)
            cache0.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++)
            cache0.remove(i);

        for (int i = 5; i >= 0; i--) {
            IgniteCache<Integer, Object> cache1 = ignite.getOrCreateCache(cacheConfiguration("cache1"));

            for (int j = 0; j < 300; j++)
                cache1.put(j + i * 100, "Cache value " + j);

            if (i != 0)
                ignite.destroyCache("cache1");
        }

        forceCheckpoint();
    }
}

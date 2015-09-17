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

package org.apache.ignite.loadtests.hashmap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

/**
 * Tests hashmap load.
 */
@SuppressWarnings("InfiniteLoopStatement")
public class GridHashMapLoadTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testHashMapLoad() {
        Map<Integer, Integer> map = new HashMap<>(5 * 1024 * 1024);

        int i = 0;

        while (true) {
            map.put(i++, i++);

            if (i % 400000 == 0)
                info("Inserted objects: " + i / 2);
        }
    }

    /**
     *
     */
    public void testConcurrentHashMapLoad() {
        Map<Integer, Integer> map = new ConcurrentHashMap<>(5 * 1024 * 1024);

        int i = 0;

        while (true) {
            map.put(i++, i++);

            if (i % 400000 == 0)
                info("Inserted objects: " + i / 2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapEntry() throws Exception {
        Map<Integer, GridCacheMapEntry> map = new HashMap<>(5 * 1024 * 1024);

        int i = 0;

        GridCacheTestContext<Integer, Integer> ctx = new GridCacheTestContext<>(
            new GridTestKernalContext(new GridTestLog4jLogger()));

        while (true) {
            Integer key = i++;
            Integer val = i++;

            map.put(key, new GridCacheMapEntry(ctx, ctx.toCacheKeyObject(key),
                key.hashCode(), ctx.toCacheObject(val), null, 1) {
                @Override public boolean tmLock(IgniteInternalTx tx, long timeout) {
                    return false;
                }

                @Override public void txUnlock(IgniteInternalTx tx) {
                    // No-op.
                }

                @Override public boolean removeLock(GridCacheVersion ver) {
                    return false;
                }
            });

            if (i % 100000 == 0)
                info("Inserted objects: " + i / 2);
        }
    }
}
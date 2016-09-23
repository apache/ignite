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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests for cache data loading during grids restart.
 */
public class CacheLoadingSequentialGridStartSelfTest extends CacheLoadingConcurrentGridStartSelfTest {
    private volatile IgniteFuture last;

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheWithDataStreamer() throws Exception {
        Ignite g0 = startGrid(0);

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 1; i < GRIDS_CNT; i++)
                    startGrid(i);

                return null;
            }
        });

        final HashSet<IgniteFuture> set = new HashSet<>();

        IgniteInClosure<Ignite> f = new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                try (IgniteDataStreamer<Integer, String> dataStreamer = grid.dataStreamer(null)) {
                    for (int i = 0; i < KEYS_CNT; i++) {
                        set.add(last = dataStreamer.addData(i, "Data"));

                        if (i % 100000 == 0)
                            log.info("Streaming "+i+"'th entry.");
                    }
                }
            }
        };

        f.apply(g0);

        log.info("Data loaded.");

        fut.get();

        last.get();

        Iterator<IgniteFuture> it = set.iterator();

        while (it.hasNext())
            it.next().get();
        //    assert it.next().get() == null;

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache = grid(0).cache(null);

        if (cache.size(CachePeekMode.PRIMARY) != KEYS_CNT){
            Set<Integer> failedKeys = new LinkedHashSet<>();

            for (int i = 0; i < KEYS_CNT; i++)
                if (!cache.containsKey(i)) {
                    for (Ignite ignite : G.allGrids()) {
                        IgniteEx igniteEx = (IgniteEx)ignite;

                        log.info(">>>>>>>>>>>>>>>>>>Aff > " +
                            igniteEx.localNode().id() +
                            " primary=" +
                            ignite.affinity(null).isPrimary(igniteEx.localNode(), i) +
                            " backup=" +
                            ignite.affinity(null).isBackup(igniteEx.localNode(), i) +
                            " local peek=" +
                            ignite.cache(null).localPeek(i, CachePeekMode.ONHEAP));
                    }

                    for (int j = i; j < i + 10000; j++)
                        if (!cache.containsKey(j))
                            failedKeys.add(j);

                    break;
                }

            assert failedKeys.isEmpty() : "Failed keys: " + failedKeys.toString();
        }

        assertCacheSize();
    }
}

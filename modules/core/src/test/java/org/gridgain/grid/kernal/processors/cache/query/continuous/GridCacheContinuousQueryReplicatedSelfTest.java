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

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Continuous queries tests for replicated cache.
 */
public class GridCacheContinuousQueryReplicatedSelfTest extends GridCacheContinuousQueryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteNodeCallback() throws Exception {
        GridCache<Integer, Integer> cache1 = grid(0).cache(null);

        GridCache<Integer, Integer> cache2 = grid(1).cache(null);

        GridCacheContinuousQuery<Integer, Integer> qry = cache2.queries().createContinuousQuery();

        final AtomicReference<Integer> val = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        qry.callback(new P2<UUID, Collection<Map.Entry<Integer, Integer>>>() {
            @Override public boolean apply(UUID uuid, Collection<Map.Entry<Integer, Integer>> entries) {
                assertEquals(1, entries.size());

                Map.Entry<Integer, Integer> e = entries.iterator().next();

                log.info("Entry: " + e);

                val.set(e.getValue());

                latch.countDown();

                return false;
            }
        });

        qry.execute();

        cache1.put(1, 10);

        latch.await(LATCH_TIMEOUT, MILLISECONDS);

        assertEquals(10, val.get().intValue());
    }
}

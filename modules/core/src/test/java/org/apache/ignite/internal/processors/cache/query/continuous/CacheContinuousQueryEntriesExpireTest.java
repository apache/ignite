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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Test continuous queries with expired entries.
 */
public class CacheContinuousQueryEntriesExpireTest extends GridCommonAbstractTest {
    /** @throws Exception If fails. */
    @Test
    public void testBackupQOnEntriesExpire() throws Exception {
        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        IgniteCache<Object, Object> cache = srv0.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(10)))
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)));

        srv1.cache(DEFAULT_CACHE_NAME).query(new ContinuousQuery<>().setLocalListener(evts -> {}).setIncludeExpired(true));

        for (int i = 0; i < 1_000; i++)
            cache.put(i, i);

        ConcurrentMap<UUID, GridContinuousProcessor.LocalRoutineInfo> locInfos =
            getFieldValue(srv1.context().continuous(), "locInfos");

        assertEquals(1, locInfos.size());

        CacheContinuousQueryHandler<?, ?> hnd = (CacheContinuousQueryHandler<?, ?>)F.first(locInfos.values()).handler();
        GridCacheContext<?, ?> cctx = srv1.context().cache().context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < cctx.affinity().partitions(); i++) {
                if (hnd.partitionBuffer(cctx, i).backupQueueSize() != 0)
                    return false;
            }

            return true;
        }, 10_000L));
    }
}

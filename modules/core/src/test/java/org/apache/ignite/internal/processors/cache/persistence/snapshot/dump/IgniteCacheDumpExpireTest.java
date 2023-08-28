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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** */
public class IgniteCacheDumpExpireTest extends AbstractCacheDumpTest {
    /** */
    public static final long TTL = 5 * 1000;

    /** */
    private static final ExpiryPolicy EXPIRY_POLICY = new ExpiryPolicy() {
        @Override public Duration getExpiryForCreation() {
            return new Duration(MILLISECONDS, TTL);
        }

        @Override public Duration getExpiryForAccess() {
            return null;
        }

        @Override public Duration getExpiryForUpdate() {
            return null;
        }
    };

    /** */
    @Parameterized.Parameter(4)
    public boolean explicit;

    /** */
    @Parameterized.Parameters(name = "nodes={0},backups={1},persistence={2},mode={3},explicit={4}")
    public static List<Object[]> params() {
        List<Object[]> res = new ArrayList<>();

        for (Object[] row : AbstractCacheDumpTest.params()) {
            for (boolean explicit: new boolean[] {true, false}) {
                res.add(new Object[row.length + 1]);

                System.arraycopy(row, 0, res.get(res.size() - 1), 0, row.length);

                res.get(res.size() - 1)[row.length] = explicit;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).setSnapshotThreadPoolSize(1);

        if (!explicit) {
            for (CacheConfiguration<?, ?> ccfg : cfg.getCacheConfiguration())
                ccfg.setExpiryPolicyFactory(() -> EXPIRY_POLICY);
        }

        return cfg;
    }

    /** */
    @Test
    public void testExplicitExpireTime() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        T2<CountDownLatch, IgniteInternalFuture<?>> latchAndFut = runDumpAsyncAndStopBeforeStart();

        Thread.sleep(TTL);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < KEYS_CNT; i++) {
                if (ign.cache(DEFAULT_CACHE_NAME).containsKey(i))
                    return false;

                if (ign.cache(CACHE_0).containsKey(i))
                    return false;

                if (ign.cache(CACHE_1).containsKey(new Key(i)))
                    return false;
            }

            return true;
        }, 2 * TTL));

        latchAndFut.get1().countDown();

        latchAndFut.get2().get();

        checkDump(ign);
    }

    /** {@inheritDoc} */
    @Override protected void putData(
        IgniteCache<Object, Object> cache,
        IgniteCache<Object, Object> grpCache0,
        IgniteCache<Object, Object> grpCache1
    ) {
        if (explicit) {
            super.putData(
                cache.withExpiryPolicy(EXPIRY_POLICY),
                grpCache0.withExpiryPolicy(EXPIRY_POLICY),
                grpCache1.withExpiryPolicy(EXPIRY_POLICY)
            );
        }
        else
            super.putData(cache, grpCache0, grpCache1);
    }
}

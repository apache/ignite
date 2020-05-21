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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class LoadHelperV2 extends GridCommonAbstractTest {
    /** */
    @Test
    public void startServer() throws Exception {
        startGrids(2);
        IgniteEx client = startClientGrid(3);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(0));

        Random random = new Random();

        int keyRange = 100;

        for (int i = 0; i < keyRange; i++)
            cache.put(i, random.nextInt());

        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                cache.get(random.nextInt(keyRange));

                cnt.incrementAndGet();
            }
        }, 4, "load");

        long duration = 20;

        for (int i = 0; i < duration; i++) {
            long old = cnt.get();

            U.sleep(1000);

            long speed = cnt.get() - old;

            System.out.println("MY speed = " + speed + " ops/sec");
        }

        stop.set(true);
        fut.get();
    }

    /** */
    @Test
    public void startServerSQL() throws Exception {
        IgniteEx grid = startGrid(0);

        IgniteCache<Integer, Integer> cache = grid.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(0)
                .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME))));

        Random random = new Random();

        int keyRange = 100;

        for (int i = 0; i < keyRange; i++) {
            cache.put(i, random.nextInt());
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                SqlFieldsQuery qry1 = new SqlFieldsQuery(
                    "SELECT _key FROM " + cache.getName() + " WHERE _key <= " +
                        new Random().nextInt(100) + " LIMIT 1")
                    .setSchema(cache.getName());

                cache.query(qry1).getAll();

                cnt.incrementAndGet();
            }
        });

        long duration = 20;

        for (int i = 0; i < duration; i++) {
            long old = cnt.get();

            U.sleep(1000);

            long speed = cnt.get() - old;

            System.out.println("MY speed = " + speed + " ops/sec");
        }

        stop.set(true);
        fut.get();
    }
}

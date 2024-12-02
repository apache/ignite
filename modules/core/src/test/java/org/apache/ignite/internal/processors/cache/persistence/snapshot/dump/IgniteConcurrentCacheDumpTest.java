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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

/** */
public class IgniteConcurrentCacheDumpTest extends AbstractCacheDumpTest {
    /** */
    @Parameterized.Parameters(name = "nodes={0},backups={1},persistence={2},mode={3},useDataStreamer={4},onlyPrimary={5},encrypted={6}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean encrypted : new boolean[]{true, false})
            for (int nodes : new int[]{2, 3})
                for (int backups : new int[]{1, 2})
                    for (boolean persistence : new boolean[]{true, false})
                        for (CacheAtomicityMode mode : CacheAtomicityMode.values())
                            params.add(new Object[]{nodes, backups, persistence, mode, false, false, encrypted});

        return params;
    }

    /** */
    @Test
    public void testDumpWithConcurrentOperations() throws Exception {
        int thCnt = 5;

        CountDownLatch initLatch = new CountDownLatch(thCnt);
        AtomicBoolean canceled = new AtomicBoolean(false);

        try (IgniteEx srv = (IgniteEx)startGridsMultiThreaded(nodes)) {
            IgniteInternalFuture<Long> opFut = GridTestUtils.runMultiThreadedAsync(() -> {
                Set<Integer> keys = new TreeSet<>();

                ThreadLocalRandom rand = ThreadLocalRandom.current();

                IntStream.range(0, KEYS_CNT).forEach(i -> {
                    int k = rand.nextInt(KEYS_CNT);

                    keys.add(k);

                    insertOrUpdate(srv, i);
                });

                initLatch.countDown();

                while (!canceled.get()) {
                    switch (rand.nextInt(3)) {
                        case 0:
                            int newKey = rand.nextInt(KEYS_CNT);

                            int iter = 0;

                            while (keys.contains(newKey) && iter < 10) {
                                newKey = rand.nextInt(KEYS_CNT);
                                iter++;
                            }

                            keys.add(newKey);

                            insertOrUpdate(srv, newKey, rand.nextInt());

                            break;
                        case 1:
                            int updKey = keys.isEmpty() ? rand.nextInt(KEYS_CNT) : keys.iterator().next();

                            insertOrUpdate(srv, updKey, rand.nextInt());

                            break;
                        case 2:
                            if (keys.isEmpty())
                                break;

                            int rmvKey = keys.isEmpty() ? rand.nextInt(KEYS_CNT) : keys.iterator().next();

                            keys.remove(rmvKey);

                            remove(srv, rmvKey);
                    }
                }
            }, thCnt, "dump-async-ops");

            assertTrue(initLatch.await(30, TimeUnit.SECONDS));

            createDump(srv);

            canceled.set(true);

            opFut.get();

            checkDumpWithCommand(srv, DMP_NAME, backups);
        }
    }
}

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

package org.apache.ignite.client;

import java.lang.invoke.SerializedLambda;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Load, capacity and performance tests.
 */
public class LoadTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /**
     * Test thin client in multi-thread environment.
     */
    @Test
    public void testMultithreading() throws Exception {
        final int THREAD_CNT = 8;
        final int ITERATION_CNT = 20;
        final int BATCH_SIZE = 1000;
        final int PAGE_CNT = 3;

        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        // No peer class loading from thin clients: we need the server to know about this class to deserialize
        // ScanQuery filter.
        srvCfg.setBinaryConfiguration(new BinaryConfiguration().setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(getClass().getName()),
            new BinaryTypeConfiguration(SerializedLambda.class.getName())
        )));

        try (Ignite ignored = Ignition.start(srvCfg);
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            ClientCache<Integer, String> cache = client.createCache("testMultithreading");

            AtomicInteger cnt = new AtomicInteger(1);

            AtomicReference<Throwable> error = new AtomicReference<>();

            Runnable assertion = () -> {
                try {
                    int rangeStart = cnt.getAndAdd(BATCH_SIZE);
                    int rangeEnd = rangeStart + BATCH_SIZE;

                    Map<Integer, String> data = IntStream.range(rangeStart, rangeEnd).boxed()
                        .collect(Collectors.toMap(i -> i, i -> String.format("String %s", i)));

                    cache.putAll(data);

                    Query<Cache.Entry<Integer, String>> qry = new ScanQuery<Integer, String>()
                        .setPageSize(data.size() / PAGE_CNT)
                        .setFilter((i, s) -> i >= rangeStart && i < rangeEnd);

                    try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                        List<Cache.Entry<Integer, String>> res = cur.getAll();

                        assertEquals("Unexpected number of entries", data.size(), res.size());

                        Map<Integer, String> act = res.stream()
                            .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

                        assertEquals("Unexpected entries", data, act);
                    }
                }
                catch (Throwable ex) {
                    error.set(ex);
                }
            };

            CountDownLatch complete = new CountDownLatch(THREAD_CNT);

            Runnable manyAssertions = () -> {
                for (int i = 0; i < ITERATION_CNT && error.get() == null; i++)
                    assertion.run();

                complete.countDown();
            };

            ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_CNT);

            IntStream.range(0, THREAD_CNT).forEach(t -> threadPool.submit(manyAssertions));

            assertTrue("Timeout", complete.await(180, TimeUnit.SECONDS));

            String errMsg = error.get() == null ? "" : error.get().getMessage();

            assertNull(errMsg, error.get());
        }
    }
}

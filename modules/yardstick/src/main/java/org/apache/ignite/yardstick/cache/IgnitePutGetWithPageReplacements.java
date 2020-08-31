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

package org.apache.ignite.yardstick.cache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.events.EventType.EVT_PAGE_REPLACEMENT_STARTED;

/**
 * Ignite benchmark that performs payload with active page replacement.
 *
 * Test scenario:
 * On setUp phase full fill cache until replace event occured,
 * after proceed to fill two times more data.
 * Execute full scan.
 *
 * On test phase fill data belonging to only 1/2 of dataregion capacity, calculated on setUp phase.
 *
 * NOTE: EVT_PAGE_REPLACEMENT_STARTED event need to be enabled on server side.
 */
public class IgnitePutGetWithPageReplacements extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Cache name. */
    private static final String CACHE_NAME = "CacheWithReplacement";

    /** In mem reg capacity. */
    private volatile int replCntr = Integer.MAX_VALUE / 2;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        int progress = 0;

        final AtomicBoolean replacement = new AtomicBoolean();

        ignite().events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                if (evt.type() == EVT_PAGE_REPLACEMENT_STARTED) {
                    replacement.set(true);

                    return false;
                }

                return true;
            }
        }, null, EVT_PAGE_REPLACEMENT_STARTED);

        int portion = 100;

        Map<Integer, TestValue> putMap = new HashMap<>(portion, 1.f);

        while (progress < 2 * replCntr) {
            putMap.clear();

            for (int i = 0; i < portion; i++)
                putMap.put(progress + i, new TestValue(progress + i));

            progress += portion;

            cache().putAll(putMap);

            if (progress % 1000 == 0)
                BenchmarkUtils.println("progress=" + progress);

            if (replacement.compareAndSet(true, false)) {
                if (replCntr != Integer.MAX_VALUE / 2)
                    throw new Exception("Invalid expected val: " + replCntr);

                replCntr = progress;

                BenchmarkUtils.println("replCntr=" + replCntr);
            }
        }

        BenchmarkUtils.println("DataRegion fullfill complete. progress=" + progress + " replCntr=" + replCntr + ".");

        int cacheSize = 0;

        try (QueryCursor cursor = cache.query(new ScanQuery())) {
            for (Object o : cursor)
                cacheSize++;
        }

        BenchmarkUtils.println("cache size=" + cacheSize);
    }

    /** */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        int portion = 100;

        Map<Integer, TestValue> putMap = new HashMap<>(portion, 1.f);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < portion; i++) {
            int val = rnd.nextInt(replCntr / 2);

            putMap.put(val, new TestValue(val));
        }

        cache().putAll(putMap);

        return true;
    }

    /**
     * Class for test purpose.
     */
    private static class TestValue implements Serializable {
        /** */
        private int id;

        /** */
        @QuerySqlField(index = true)
        private final byte[] payload = new byte[64];

        /**
         * @param id ID.
         */
        private TestValue(int id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public int getId() {
            return id;
        }

        /**
         * @return Payload.
         */
        public boolean hasPayload() {
            return payload != null;
        }
    }
}

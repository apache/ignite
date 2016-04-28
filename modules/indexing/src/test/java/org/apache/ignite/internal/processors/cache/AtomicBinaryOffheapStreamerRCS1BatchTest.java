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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;

/**
 * Write batch test using streamer with on-heap row cache size of 1.
 */
public class AtomicBinaryOffheapStreamerRCS1BatchTest extends AtomicBinaryOffheapBaseBatchTest {
    /**
     * Size of batch in operation
     */
    public static final int BATCH_SIZE = 1_000;

    /** {@inheritDoc} */
    @Override protected int onHeapRowCacheSize() {
        return 1;
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    public void testBatchOperations() throws Exception {
        fail("IGNITE-2982");

        try (IgniteCache<Object, Object> dfltCache = ignite(0).cache(null)) {
            loadingCacheAnyDate();

            int runsCnt = 50;

            for (int cnt = 0; cnt < runsCnt; cnt++) {
                Map<Integer, Person> putMap1 = new TreeMap<>();

                for (int i = 0; i < BATCH_SIZE; i++)
                    putMap1.put(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), i / 0.99));

                dfltCache.putAll(putMap1);

                Map<Integer, Organization> putMap2 = new TreeMap<>();

                for (int i = BATCH_SIZE / 2; i < BATCH_SIZE * 3 / 2; i++) {
                    dfltCache.remove(i);

                    putMap2.put(i, new Organization(i, String.valueOf(i)));
                }

                dfltCache.putAll(putMap2);
            }
        }
    }

    /**
     * Loading date into cache
     */
    private void loadingCacheAnyDate() {
        try (IgniteDataStreamer<Object, Object> streamer = ignite(0).dataStreamer(null)) {
            for (int i = 0; i < 30_000; i++) {
                if (i % 2 == 0)
                    streamer.addData(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), i / 0.99));
                else
                    streamer.addData(i, new Organization(i, String.valueOf(i)));
            }
        }
    }
}

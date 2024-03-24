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
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_COLLECTION_LIMIT;

/** */
public class CacheDistributedGetLongRunningFutureDumpTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "5000")
    public void test() throws Exception {
        IgniteEx ignite = startGrids(3);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL));

        int logColElementsLimit = getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, DFLT_TO_STRING_COLLECTION_LIMIT);

        Set<Key> keys = new TreeSet<>();

        for (int i = 0; i < logColElementsLimit * 10; i++) {
            Key key = new Key(i);

            keys.add(key);

            cache.put(key, i);
        }

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridNearGetResponse);

        CompletableFuture<String> longRunningOpsLoggedFut = new CompletableFuture<>();

        listeningLog.registerListener(str -> {
            if (str.startsWith(">>> Future [")) {
                longRunningOpsLoggedFut.complete(str);
            }
        });

        IgniteFuture<Map<Object, Object>> getAllFut = cache.getAllAsync(keys);

        String longRunningOps = longRunningOpsLoggedFut.get(getTestTimeout(), MILLISECONDS);

        Matcher reducedEntriesMatcher = Pattern.compile("rdc=Map Reducer \\[reducedEntries=ConcurrentHashMap \\{(.+)\\}")
            .matcher(longRunningOps);

        assertTrue(reducedEntriesMatcher.find());

        assertEquals(logColElementsLimit, reducedEntriesMatcher.group(1).split(", ").length);

        spi(grid(1)).stopBlock();

        getAllFut.get(getTestTimeout());
    }

    /** */
    public static class Key implements Comparable<Key> {
        /** */
        private final int val;

        /** */
        public Key(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Key-Data-" + val;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Key o) {
            return Integer.compare(val, o.val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            return val == ((Key)o).val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(val);
        }
    }
}


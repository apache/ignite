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
import java.util.Random;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
@RunWith(JUnit4.class)
public class CacheBlockOnGetAllTest extends CacheBlockOnReadAbstractTest {

    /** {@inheritDoc} */
    @Override @NotNull protected CacheReadBackgroundOperation<?, ?> getReadOperation() {
        return new IntCacheReadBackgroundOperation() {
            /** Random. */
            private Random random = new Random();

            /** {@inheritDoc} */
            @Override public void doRead() {
                Set<Integer> keys = new HashSet<>();

                for (int i = 0; i < 500; i++)
                    keys.add(random.nextInt(entriesCount()));

                cache().getAll(keys);
            }
        };
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopBaselineAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9915");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testStopBaselineAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9915");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopBaselineTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9915");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testStopBaselineTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9915");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testCreateCacheAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testCreateCacheAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testCreateCacheTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testCreateCacheTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testDestroyCacheAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testDestroyCacheAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testDestroyCacheTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testDestroyCacheTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testStartServerAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testStartServerAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testStartServerTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testStartServerTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopServerAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testStopServerAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopServerTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testStopServerTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testUpdateBaselineTopologyAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testUpdateBaselineTopologyAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testUpdateBaselineTopologyTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testUpdateBaselineTopologyTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9883");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testStartClientAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testStartClientAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testStartClientTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testStartClientTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopClientAtomicPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Test
    @Override public void testStopClientAtomicReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Test
    @Override public void testStopClientTransactionalPartitioned() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Test
    @Override public void testStopClientTransactionalReplicated() {
        fail("https://issues.apache.org/jira/browse/IGNITE-9987");
    }
}

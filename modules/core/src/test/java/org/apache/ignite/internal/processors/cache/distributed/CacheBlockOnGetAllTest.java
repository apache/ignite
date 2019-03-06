/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9915")
    @Test
    @Override public void testStopBaselineAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9915")
    @Test
    @Override public void testStopBaselineAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9915")
    @Test
    @Override public void testStopBaselineTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9915")
    @Test
    @Override public void testStopBaselineTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testCreateCacheAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testCreateCacheAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testCreateCacheTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testCreateCacheTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testDestroyCacheAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testDestroyCacheAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testDestroyCacheTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testDestroyCacheTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStartServerAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStartServerAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStartServerTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStartServerTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStopServerAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStopServerAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStopServerTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testStopServerTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testUpdateBaselineTopologyAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testUpdateBaselineTopologyAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testUpdateBaselineTopologyTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9883")
    @Test
    @Override public void testUpdateBaselineTopologyTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStartClientAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStartClientAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStartClientTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStartClientTransactionalReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStopClientAtomicPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStopClientAtomicReplicated() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStopClientTransactionalPartitioned() {
        // No-op
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9987")
    @Test
    @Override public void testStopClientTransactionalReplicated() {
        // No-op
    }
}

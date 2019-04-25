/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheContinuousQueryFailoverMvccTxSelfTest extends CacheContinuousQueryFailoverAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL_SNAPSHOT;
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7311")
    @Test
    @Override public void testBackupQueueEvict() throws Exception {
        // No-op.
    }
}

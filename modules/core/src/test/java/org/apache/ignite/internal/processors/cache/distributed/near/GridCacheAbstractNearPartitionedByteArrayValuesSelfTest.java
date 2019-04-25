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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractPartitionedByteArrayValuesSelfTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for byte array values in NEAR-PARTITIONED caches.
 */
public abstract class GridCacheAbstractNearPartitionedByteArrayValuesSelfTest extends
    GridCacheAbstractPartitionedByteArrayValuesSelfTest {
    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    @Override public void testPessimisticMvcc() throws Exception {
        super.testPessimisticMvcc();
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    @Override public void testPessimisticMvccMixed() throws Exception {
        super.testPessimisticMvccMixed();
    }
}

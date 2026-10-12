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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor.TREE_CORRUPTION_REG_NAME;

/**
 * Tests that the B+Tree cursor corruption metric is properly registered.
 */
public class TreeCorruptionMetricTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(4))
        );

        return cfg;
    }

    /**
     * Verifies that the treeCorruption metric is registered and starts at 0.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricRegistered() throws Exception {
        IgniteEx ignite = startGrid(0);

        LongMetric m = ignite.context().metric().registry(TREE_CORRUPTION_REG_NAME).findMetric("count");

        assertNotNull(m);
        assertEquals(0, m.value());
    }
}

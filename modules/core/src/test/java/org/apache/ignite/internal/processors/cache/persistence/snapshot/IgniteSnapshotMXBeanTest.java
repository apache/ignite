/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;

/**
 * Tests {@link SnapshotMXBean}.
 */
public class IgniteSnapshotMXBeanTest extends AbstractSnapshotSelfTest {
    /** @throws Exception If fails. */
    @Test
    public void testCreateSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        SnapshotMXBean mxBean = getMBean(ignite.name());

        mxBean.createSnapshot(SNAPSHOT_NAME);

        MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_METRICS);

        LongMetric endTime = mreg.findMetric("LastSnapshotEndTime");

        assertEquals("Snapshot end time must be undefined on first snapshot operation starts.",
            0, endTime.value());

        assertTrue("Waiting for snapshot operation failed.",
            GridTestUtils.waitForCondition(() -> endTime.value() > 0, 10_000));

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /**
     * @param ignite Ignite instance name.
     * @return Snapshot MBean.
     */
    private SnapshotMXBean getMBean(String ignite) {
        return getMxBean(ignite, "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);
    }
}

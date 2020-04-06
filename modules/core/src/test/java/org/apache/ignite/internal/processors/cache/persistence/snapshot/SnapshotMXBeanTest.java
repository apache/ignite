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

import java.util.Collections;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.junit.Test;

/**
 * Tests {@link SnapshotMXBean}.
 */
public class SnapshotMXBeanTest extends AbstractSnapshotSelfTest {
    /** @throws Exception If fails. */
    @Test
    public void testCreateSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        SnapshotMXBean mxBean = getMBean(ignite.name());

        mxBean.createSnapshot(SNAPSHOT_NAME);

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testListOfSnapshots() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        SnapshotMXBean mxBean = getMBean(ignite.name());

        mxBean.createSnapshot(SNAPSHOT_NAME);

        assertEquals("Snapshot must be created",
            Collections.singletonList(SNAPSHOT_NAME), mxBean.getSnapshots());
    }

    /**
     * @param ignite Ignite instance name.
     * @return Snapshot MBean.
     */
    private SnapshotMXBean getMBean(String ignite) {
        return getMxBean(ignite, "Snapshot", SnapshotMXBeanImpl.class, SnapshotMXBean.class);
    }
}

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

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Cluster-wide snapshot test check command with indexes.
 */
public class IgniteClusterSnapshotCheckWithIndexesTest extends AbstractSnapshotSelfTest {
    private final CacheConfiguration<Integer, Account> indexedCcfg =
        txCacheConfig(new CacheConfiguration<Integer, Account>("indexed"))
            .setQueryEntities(singletonList(new QueryEntity(Integer.class.getName(), Account.class.getName())));

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithIndexes() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, key -> new Account(key, key), indexedCcfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        IdleVerifyResultV2 res = ignite.context().cache().context().snapshotMgr().checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found.");
    }
}

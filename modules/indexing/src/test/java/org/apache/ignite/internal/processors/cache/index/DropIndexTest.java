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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState.State.COMPLETED;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for testing index drop.
 */
public class DropIndexTest extends AbstractRebuildIndexTest {
    @Test
    public void test00() throws Exception {
        IgniteEx n = startGrid(0);

        IgniteCache<Integer, Person> cache = n.cache(DEFAULT_CACHE_NAME);

        populate(cache, 100);

        //enableCheckpoints(n, getTestIgniteInstanceName(), false);

        String idxName = "IDX0";

        createIdx(cache, idxName);

        enableCheckpoints(n, getTestIgniteInstanceName(), false);

        dropIdx(cache, idxName);

        n.cluster().baselineAutoAdjustEnabled(false);
        stopGrid(0);

        n = startGrid(0, cfg -> {
            cfg.setClusterStateOnStart(INACTIVE);
        });

        DurableBackgroundTaskState state = dropIdxTask(n, idxName);
        assertNotNull(state);

        n.cluster().state(ACTIVE);

        assertTrue(waitForCondition(() -> state.state() == COMPLETED, getTestTimeout()));
    }

    /**
     * Getting the {@link DurableBackgroundCleanupIndexTreeTask} for the index.
     *
     * @param n Node.
     * @param idxName Index name.
     * @return Status with {@link DurableBackgroundCleanupIndexTreeTask} for the index.
     */
    @Nullable private DurableBackgroundTaskState dropIdxTask(IgniteEx n, String idxName) {
        Map<String, DurableBackgroundTaskState> tasks = getFieldValue(n.context().durableBackgroundTask(), "tasks");

        return tasks.values().stream()
            .filter(s -> s.task() instanceof DurableBackgroundCleanupIndexTreeTask)
            .filter(s -> s.task().name().contains(idxName))
            .findAny()
            .orElse(null);
    }

    /**
     * Drop of an index for the cache of{@link Person}.
     * SQL: DROP INDEX " + idxName
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation future.
     */
    private List<List<?>> dropIdx(IgniteCache<Integer, Person> cache, String idxName) {
        return cache.query(new SqlFieldsQuery("DROP INDEX " + idxName)).getAll();
    }
}

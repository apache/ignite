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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexTarget;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.INDEX_REBUILD_MNTC_TASK_NAME;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.INDEX_REBUILD_PARAMETER_SEPARATOR;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.mergeTasks;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.parseMaintenanceTaskParameters;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;

/** Tests for {@link MaintenanceRebuildIndexTarget}. */
public class MaintenanceRebuildIndexUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * Tests that maintenance task's parameters can be stringified and parsed back.
     */
    @Test
    public void testSerializeAndParse() {
        int cacheId = 1;
        String idxName = "test";

        MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, task.name());

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(1, targets.size());

        MaintenanceRebuildIndexTarget target = targets.get(0);

        assertEquals(cacheId, target.cacheId());
        assertEquals(idxName, target.idxName());
    }

    /**
     * Tests that maintenance task's parameters can be merged correctly.
     */
    @Test
    public void testMerge() {
        List<MaintenanceRebuildIndexTarget> targets = IntStream.range(0, 100)
            .mapToObj(i -> new MaintenanceRebuildIndexTarget(i, "idx" + i)).collect(toList());

        MaintenanceRebuildIndexTarget first = targets.get(0);

        // Create initial task
        MaintenanceTask task = toMaintenanceTask(first.cacheId(), first.idxName());

        // Merge all tasks in one task
        for (MaintenanceRebuildIndexTarget target : targets)
            task = mergeTasks(task, toMaintenanceTask(target.cacheId(), target.idxName()));

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, task.name());

        List<MaintenanceRebuildIndexTarget> parsedTargets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(targets, parsedTargets);
    }

    /**
     * Tests that merging same tasks yields a correct task without duplication of its parameters.
     */
    @Test
    public void testMergeSame() {
        int cacheId = 1;
        String idxName = "test";

        MaintenanceTask task1 = toMaintenanceTask(cacheId, idxName);
        MaintenanceTask task2 = toMaintenanceTask(cacheId, idxName);

        MaintenanceTask mergedTask = mergeTasks(task1, task2);

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, mergedTask.name());

        assertEquals(task1.parameters(), mergedTask.parameters());
    }

    /**
     * Tests that {@link MaintenanceRebuildIndexUtils#INDEX_REBUILD_PARAMETER_SEPARATOR} can be used in the index name.
     */
    @Test
    public void testIndexNameWithSeparatorCharacter() {
        int cacheId = 1;
        String idxName = "test" + INDEX_REBUILD_PARAMETER_SEPARATOR + "test";

        MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(1, targets.size());

        MaintenanceRebuildIndexTarget target = targets.get(0);

        assertEquals(cacheId, target.cacheId());
        assertEquals(idxName, target.idxName());
    }

    /**
     * Tests that maintenance task can be constructed from a map.
     */
    @Test
    public void testConstructFromMap() {
        Map<Integer, Set<String>> cacheToIndexes = new HashMap<>();
        cacheToIndexes.put(1, new HashSet<>(Arrays.asList("foo", "bar")));
        cacheToIndexes.put(2, new HashSet<>(Arrays.asList("foo1", "bar1")));

        MaintenanceTask task = toMaintenanceTask(cacheToIndexes);

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(4, targets.size());

        Map<Integer, Set<String>> result = targets.stream().collect(groupingBy(
            MaintenanceRebuildIndexTarget::cacheId,
            mapping(MaintenanceRebuildIndexTarget::idxName, toSet())
        ));

        assertEqualsMaps(cacheToIndexes, result);
    }
}

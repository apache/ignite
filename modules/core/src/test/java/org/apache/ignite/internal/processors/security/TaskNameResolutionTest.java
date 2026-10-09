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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** Tests task metadata lookup. */
public class TaskNameResolutionTest extends AbstractSecurityTest {
    /** Task name. */
    private static final String TASK_NAME = "test-task";

    /** */
    @Test
    public void testResolveTaskNameOnInactiveCluster() throws Exception {
        IgniteEx ignite = startGridAllowAll(getTestIgniteInstanceName(0));

        startGridAllowAll(getTestIgniteInstanceName(1));

        awaitPartitionMapExchange();

        assertTaskNameResolved(ignite);

        ignite.cluster().state(ClusterState.INACTIVE);

        assertNull(grid(1).context().task().resolveTaskName(TASK_NAME.hashCode()));

        ignite.cluster().state(ClusterState.ACTIVE);

        assertTaskNameResolved(ignite);
    }

    /** Executes a named task and checks that its name can be resolved on another node. */
    private void assertTaskNameResolved(IgniteEx ignite) {
        ignite.compute().withName(TASK_NAME).run(() -> {});

        assertEquals(TASK_NAME, grid(1).context().task().resolveTaskName(TASK_NAME.hashCode()));
    }
}

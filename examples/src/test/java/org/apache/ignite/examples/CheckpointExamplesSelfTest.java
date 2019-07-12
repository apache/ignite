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

package org.apache.ignite.examples;

import org.apache.ignite.examples.computegrid.failover.ComputeFailoverExample;
import org.apache.ignite.examples.computegrid.failover.ComputeFailoverNodeStartup;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 * Checkpoint examples self test.
 */
public class CheckpointExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * Starts remote nodes before each test.
     *
     * Note: using beforeTestsStarted() to start nodes only once won't work.
     *
     * @throws Exception If remote nodes start failed.
     */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid("node-" + i, ComputeFailoverNodeStartup.configuration());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointExample() throws Exception {
        ComputeFailoverExample.main(EMPTY_ARGS);
    }
}

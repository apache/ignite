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

package org.apache.ignite.distributed;

import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Distributed transaction test using a single partition table, 3 nodes and 3 replicas.
 */
public class ItTxDistributedTestThreeNodesThreeReplicas extends ItTxDistributedTestSingleNode {
    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestThreeNodesThreeReplicas(TestInfo testInfo) {
        super(testInfo);
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int replicas() {
        return 3;
    }

    @Override
    @AfterEach
    public void after() throws Exception {
        IgniteTestUtils.waitForCondition(() -> assertPartitionsSame(accounts, 0), 5_000);
        IgniteTestUtils.waitForCondition(() -> assertPartitionsSame(customers, 0), 5_000);

        super.after();
    }
}

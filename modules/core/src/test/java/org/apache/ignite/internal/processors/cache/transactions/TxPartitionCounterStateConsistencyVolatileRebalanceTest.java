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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.junit.Ignore;

/**
 * Test partitions consistency in various scenarios when all rebalance is in-memory.
 */
public class TxPartitionCounterStateConsistencyVolatileRebalanceTest extends TxPartitionCounterStateConsistencyTest {
    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Ignore
    @Override public void testSingleThreadedUpdateOrder() throws Exception {
        // Not applicable for volatile mode.
    }

    /** {@inheritDoc} */
    @Ignore
    @Override public void testPartitionConsistencyCancelledRebalanceCoordinatorIsDemander() throws Exception {
        // Not applicable for volatile mode.
    }

    /** {@inheritDoc} */
    @Override protected void forceCheckpoint(Collection<Ignite> nodes) throws IgniteCheckedException {
        // No-op.
    }
}

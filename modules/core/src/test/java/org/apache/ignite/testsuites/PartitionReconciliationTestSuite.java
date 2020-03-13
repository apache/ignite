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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.checker.ConsistencyCheckUtilsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationBinaryObjectsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFastCheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationFullFixStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRecheckTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationInterruptionRepairTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationRecheckAttemptsTest;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationStressTest;
import org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResourceLimitedJobTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTaskTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTaskTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationAtomicTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationCommonTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationExtendedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationReplicatedTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxPersistentTest;
import org.apache.ignite.util.GridCommandHandlerPartitionReconciliationTxTest;

/**
 * Test suite.
 */
public class PartitionReconciliationTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("PartitionReconciliationTestSuite");

        suite.addTestSuite(GridCommandHandlerPartitionReconciliationTxTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationTxPersistentTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationReplicatedTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationReplicatedPersistentTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationExtendedTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationCommonTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationAtomicTest.class);
        suite.addTestSuite(GridCommandHandlerPartitionReconciliationAtomicPersistentTest.class);
        suite.addTestSuite(PartitionReconciliationInterruptionRecheckTest.class);
        suite.addTestSuite(PartitionReconciliationInterruptionRepairTest.class);
        suite.addTestSuite(PartitionReconciliationProcessorTest.class);
        suite.addTestSuite(PartitionReconciliationRecheckAttemptsTest.class);
        suite.addTestSuite(PartitionReconciliationStressTest.class);
        suite.addTestSuite(PartitionReconciliationFixStressTest.class);
        suite.addTestSuite(PartitionReconciliationBinaryObjectsTest.class);
        suite.addTestSuite(CollectPartitionKeysByBatchTaskTest.class);
        suite.addTestSuite(PartitionReconciliationFullFixStressTest.class);
        suite.addTestSuite(CollectPartitionKeysByRecheckRequestTaskTest.class);
        suite.addTestSuite(ConsistencyCheckUtilsTest.class);
        suite.addTestSuite(RepairEntryProcessorTest.class);
        suite.addTestSuite(RepairRequestTaskTest.class);
        suite.addTestSuite(ReconciliationResourceLimitedJobTest.class);
        suite.addTestSuite(PartitionReconciliationFastCheckTest.class);

        return suite;
    }
}

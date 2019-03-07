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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FAIL_NODE_ON_UNRECOVERABLE_PARTITION_INCONSISTENCY;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Tests partition consistency recovery in case then all owners are lost in the middle of transaction.
 *
 * TODO FIXME add test for consistency check when nodes are not failed by handler.
 */
public class TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest extends
    TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        int histRebCnt = IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances().size();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        super.afterTest();

        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
    }

    @Override public void testStopAllOwnersWithPartialCommit_3_1() throws Exception {
        super.testStopAllOwnersWithPartialCommit_3_1();
    }
}

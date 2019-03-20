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

import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 *
 */
public class TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest extends TxPartitionCounterStateOnePrimaryTwoBackupsTest {
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

        // Expecting only one historical rebalance for test scenario.
        assertEquals("WAL rebalance must happen exactly 1 time", 1, histRebCnt);
    }
}

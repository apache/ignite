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
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE;

/**
 *
 */
@WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
public class TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest
    extends TxPartitionCounterStateOnePrimaryTwoBackupsTest {
    /** {@inheritDoc} */
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        int histRebCnt = IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances().size();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        super.afterTest();

        // Expecting only one historical rebalance for test scenario.
        assertEquals("WAL rebalance must happen exactly 1 time", 1, histRebCnt);
    }

    /** {@inheritDoc} */
    @Test
    @Ignore
    @Override public void testMissingUpdateBetweenMultipleCheckpoints() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Ignore
    @Override public void testCommitReorderWithRollbackNoRebalanceAfterRestart() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_1_1() throws Exception {
        super.testPartialPrepare_2TX_1_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_1_2() throws Exception {
        super.testPartialPrepare_2TX_1_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_1_3() throws Exception {
        super.testPartialPrepare_2TX_1_3();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_1_4() throws Exception {
        super.testPartialPrepare_2TX_1_4();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_2_1() throws Exception {
        super.testPartialPrepare_2TX_2_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_2_2() throws Exception {
        super.testPartialPrepare_2TX_2_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_2_3() throws Exception {
        super.testPartialPrepare_2TX_2_3();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_2TX_2_4() throws Exception {
        super.testPartialPrepare_2TX_2_4();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_1_1() throws Exception {
        super.testPartialPrepare_3TX_1_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_2_1() throws Exception {
        super.testPartialPrepare_3TX_2_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_3_1() throws Exception {
        super.testPartialPrepare_3TX_3_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_4_1() throws Exception {
        super.testPartialPrepare_3TX_4_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_5_1() throws Exception {
        super.testPartialPrepare_3TX_5_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_6_1() throws Exception {
        super.testPartialPrepare_3TX_6_1();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_1_2() throws Exception {
        super.testPartialPrepare_3TX_1_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_2_2() throws Exception {
        super.testPartialPrepare_3TX_2_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_3_2() throws Exception {
        super.testPartialPrepare_3TX_3_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_4_2() throws Exception {
        super.testPartialPrepare_3TX_4_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_5_2() throws Exception {
        super.testPartialPrepare_3TX_5_2();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("Rebalance may be not triggered because all prepared txs may have counters greater than commited tx's " +
        "and will be rolled back on recovery, so, nothing to rebalance")
    @Override public void testPartialPrepare_3TX_6_2() throws Exception {
        super.testPartialPrepare_3TX_6_2();
    }
}

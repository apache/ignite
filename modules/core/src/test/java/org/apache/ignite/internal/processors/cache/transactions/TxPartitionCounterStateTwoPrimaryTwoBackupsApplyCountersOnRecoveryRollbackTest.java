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

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

/**
 */
public class TxPartitionCounterStateTwoPrimaryTwoBackupsApplyCountersOnRecoveryRollbackTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int BACKUPS = 1;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testPrepareCommitReorder() throws Exception {
        doTestPrepareCommitReorder(false);
    }

    /** */
    @Test
    public void testPrepareCommitReorderSkipCheckpoint() throws Exception {
        doTestPrepareCommitReorder(true);
    }

    /**
     * Test scenario
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestPrepareCommitReorder(boolean skipCheckpoint) throws Exception {
        T2<Ignite, List<Ignite>> txTop = runOnPartition(PARTITION_ID, new Supplier<Integer>() {
                @Override public Integer get() {
                    return PARTITION_ID + 1;
                }
            }, BACKUPS, NODES_CNT,
            new IgniteClosure2X<Ignite, List<Ignite>, TxPartitionCounterStateAbstractTest.TxCallback>() {
                @Override public TxPartitionCounterStateAbstractTest.TxCallback applyx(Ignite primary,
                    List<Ignite> backups) throws IgniteCheckedException {
                    return new TxCallbackAdapter() {
                    };
                }
            },
            SIZES);

        assertEquals(TOTAL + KEYS_IN_SECOND_PARTITION, grid(CLIENT_GRID_NAME).cache(DEFAULT_CACHE_NAME).size());
    }
}

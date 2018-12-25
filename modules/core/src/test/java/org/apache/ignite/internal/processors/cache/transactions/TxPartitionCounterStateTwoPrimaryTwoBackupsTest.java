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
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 */
@RunWith(JUnit4.class)
public class TxPartitionCounterStateTwoPrimaryTwoBackupsTest extends TxPartitionCounterStateAbstractTest {
    /** */
    private static final int [] SIZES = new int[] {5, 7, 3};

    /** */
    private static final int TOTAL = IntStream.of(SIZES).sum() + PRELOAD_KEYS_CNT;

    /** */
    private static final int PARTITION_ID = 0;

    /**
     * Choose second partition to enforce condition: primary nodes are different, backup is same.
     */
    private static final int PARTITION_ID_2 = PARTITION_ID + 5;

    /** */
    private static final int BACKUPS = 1;

    /** */
    private static final int NODES_CNT = 3;

    /** */
    @Test
    public void testFailoverOnPrepare2Partitions() throws Exception {
        doTestFailoverOnPrepare2Partitions(false);
    }

    /** */
    @Test
    public void testFailoverOnPrepare2PartitionsSkipCheckpoint() throws Exception {
        doTestFailoverOnPrepare2Partitions(true);
    }

    /**
     * Test scenario:
     *
     * txs prepared in order 0, 1, 2
     * tx[2] committed out of order.
     * tx[0], tx[1] rolled back due to prepare fail.
     *
     * Pass: counters for rolled back txs are incremented on primary and backup nodes.
     *
     * @param skipCheckpoint Skip checkpoint.
     */
    private void doTestFailoverOnPrepare2Partitions(boolean skipCheckpoint) throws Exception {
        final int finishedTxIdx = 2;

        Map<Integer, T2<Ignite, List<Ignite>>> txTop = runOnPartition(PARTITION_ID, new Supplier<Integer>() {
                @Override public Integer get() {
                    return PARTITION_ID_2;
                }
            }, BACKUPS, NODES_CNT,
            new IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback>() {
                @Override public TxCallback apply(Map<Integer, T2<Ignite, List<Ignite>>> txTop) {
                    return new TxCallbackAdapter() {
                        /** */
                        private Queue<Integer> prepOrder = new ConcurrentLinkedQueue<Integer>();

                        {
                            prepOrder.add(0);
                            prepOrder.add(1);
                            prepOrder.add(2);
                        }

                        /** */
                        private Map<IgniteUuid, GridFutureAdapter<?>> prepFuts = new ConcurrentHashMap<>();

                        /** {@inheritDoc} */
                        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
                            GridFutureAdapter<?> proceedFut) {
                            if (txTop.get(PARTITION_ID).get1() == primary) { // Order prepare for part1
                                runAsync(() -> {
                                    prepFuts.put(nearXidVer, proceedFut);

                                    // Order prepares.
                                    if (prepFuts.size() == SIZES.length) {// Wait until all prep requests queued and force prepare order.
                                        prepFuts.remove(version(prepOrder.poll())).onDone();
                                    }
                                });

                                return true;
                            }

                            return order(nearXidVer) != finishedTxIdx; // Delay txs 0 and 1 for part2, allow tx 2 to finish.
                        }

                        /** {@inheritDoc} */
                        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
                            GridFutureAdapter<?> fut) {
                            if (txTop.get(PARTITION_ID).get1() == primary) {
                                runAsync(() -> {
                                    log.info("TX: Prepared part1: " + order(nearXidVer));

                                    if (prepOrder.isEmpty()) {
                                        log.info("TX: All prepared part1");

                                        // fail primary for second partition and trigger rollback for prepared transactions on.
                                        stopGrid(skipCheckpoint, txTop.get(PARTITION_ID_2).get1().name());

                                        TestRecordingCommunicationSpi.stopBlockAll();

                                        return;
                                    }

                                    prepFuts.remove(version(prepOrder.poll())).onDone();
                                });
                            }

                            return order(nearXidVer) != finishedTxIdx; // Delay final preparation for tx[0] and tx[1]
                        }
                    };
                }
            },
            SIZES);

        // Expect only one committed tx.
        assertEquals(PRELOAD_KEYS_CNT + SIZES[finishedTxIdx], grid(CLIENT_GRID_NAME).cache(DEFAULT_CACHE_NAME).size());

        // Expect consistent partitions.
        assertPartitionsSame(idleVerify(grid(CLIENT_GRID_NAME), DEFAULT_CACHE_NAME));

        PartitionUpdateCounter pc0 = null;

        // Expect same counters on primary and backup.
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            PartitionUpdateCounter pc = counter(PARTITION_ID, ignite.name());

            if (pc0 == null)
                pc0 = pc;
            else {
                assertEquals(pc0, pc);

                pc0 = pc;
            }
        }
    }
}

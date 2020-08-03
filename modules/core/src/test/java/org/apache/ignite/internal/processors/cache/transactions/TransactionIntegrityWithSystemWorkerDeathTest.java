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

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class TransactionIntegrityWithSystemWorkerDeathTest extends AbstractTransactionIntergrityTest {
    /** {@inheritDoc}. */
    @Override protected long getTestTimeout() {
        return 60 * 1000L;
    }

    /** {@inheritDoc}. */
    @Override protected boolean persistent() {
        return false;
    }

    /** */
    @Test
    public void testFailoverWithDiscoWorkerTermination() throws Exception {
        doTestTransferAmount(new FailoverScenario() {
            static final int failedNodeIdx = 1;

            /** {@inheritDoc}. */
            @Override public void afterFirstTransaction() throws Exception {
                // Terminate disco-event-worker thread on one node.
                WorkersControlMXBean bean = getMxBean(getTestIgniteInstanceName(failedNodeIdx), "Kernal",
                    WorkersControlMXBeanImpl.class, WorkersControlMXBean.class);

                bean.terminateWorker(
                    bean.getWorkerNames().stream()
                        .filter(name -> name.startsWith("disco-event-worker"))
                        .findFirst()
                        .orElse(null)
                );
            }

            /** {@inheritDoc}. */
            @Override public void afterTransactionsFinished() throws Exception {
                // Wait until node with death worker will left cluster.
                GridTestUtils.waitForCondition(() -> {
                    try {
                        grid(failedNodeIdx);
                    }
                    catch (IgniteIllegalStateException e) {
                        return true;
                    }

                    return false;
                }, getTestTimeout());

                // Failed node should be stopped.
                GridTestUtils.assertThrows(log, () -> grid(failedNodeIdx), IgniteIllegalStateException.class, "");

                // Re-start failed node.
                startGrid(failedNodeIdx);

                awaitPartitionMapExchange();
            }
        }, true);
    }
}

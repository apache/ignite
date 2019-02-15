/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
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
                WorkersControlMXBean bean = workersMXBean(failedNodeIdx);

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

    /**
     * Configure workers mx bean.
     */
    private WorkersControlMXBean workersMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(
            getTestIgniteInstanceName(igniteInt),
            "Kernal",
            WorkersControlMXBeanImpl.class.getSimpleName()
        );

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, WorkersControlMXBean.class, true);
    }
}

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

package org.apache.ignite.failure;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.query.schema.SchemaExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test of triggering of failure handler.
 */
public class FailureHandlerTriggeredTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailureHandlerTriggeredOnExchangeWorkerTermination() throws Exception {
        try {
            CountDownLatch latch = new CountDownLatch(1);

            TestFailureHandler hnd = new TestFailureHandler(false, latch);

            IgniteEx ignite = startGrid(getConfiguration().setFailureHandler(hnd));

            GridCachePartitionExchangeManager<Object, Object> exchangeMgr = ignite.context().cache().context().exchange();

            GridWorker exchangeWorker =
                GridTestUtils.getFieldValue(exchangeMgr, GridCachePartitionExchangeManager.class, "exchWorker");

            assertNotNull(exchangeWorker);

            GridTestUtils.invoke(exchangeWorker, "addCustomTask", new ExchangeWorkerFailureTask());

            assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));

            assertNotNull(hnd.failureCtx);

            assertEquals(hnd.failureCtx.type(), FailureType.SYSTEM_WORKER_TERMINATION);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Custom exchange worker task implementation for delaying exchange worker processing.
     */
    static class ExchangeWorkerFailureTask extends SchemaExchangeWorkerTask implements CachePartitionExchangeWorkerTask {
        /**
         * Default constructor.
         */
        ExchangeWorkerFailureTask() {
            super(new SchemaAbstractDiscoveryMessage(null) {
                @Override public boolean exchange() {
                    return false;
                }

                @Nullable @Override public DiscoveryCustomMessage ackMessage() {
                    return null;
                }

                @Override public boolean isMutable() {
                    return false;
                }

                @Override public boolean stopProcess() {
                    return false;
                }
            });
        }

        /** {@inheritDoc} */
        @Override public SchemaAbstractDiscoveryMessage message() {
            throw new Error("Exchange worker termination");
        }
    }
}

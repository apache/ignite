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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 * Tests that an explicit lock request cancelled on a stopping node does not cause a critical failure
 * when the response for the in-flight local DHT lock arrives after the cancellation.
 */
public class ExplicitLockCancelOnNodeStopTest extends GridCommonAbstractTest {
    /** */
    private volatile RunnableX beforeStop;

    /** */
    private final TestFailureHandler failureHnd = new TestFailureHandler(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1))
            .setLifecycleBeans(evt -> {
                if (evt == LifecycleEventType.BEFORE_NODE_STOP && getTestIgniteInstanceName(0).equals(igniteInstanceName))
                    beforeStop.run();
            });
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return failureHnd;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Scenario:
     * <ol>
     *     <li>A thread holds an explicit lock on one key so its explicit lock span stays non-empty.</li>
     *     <li>The node starts stopping and, from the {@code BEFORE_NODE_STOP} callback, the same thread tries to
     *     lock another key. The local DHT lock is acquired and a lock request is sent to the backup.</li>
     *     <li>Since the node is already stopping, the lock future is cancelled immediately which removes
     *     the explicit lock candidate from the span.</li>
     *     <li>The backup response arrives afterwards and the completed DHT lock future tries to mark
     *     the already removed candidate as owned.</li>
     * </ol>
     */
    @Test
    public void testLockOnStoppingNode() throws Exception {
        Ignite srv = startGrid(0);
        Ignite backup = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = srv.cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = primaryKeys(cache, 2);

        cache.lock(keys.get(0)).lock();

        spi(backup).blockMessages(GridDhtLockResponse.class, srv.name());

        AtomicReference<Throwable> lockErr = new AtomicReference<>();

        beforeStop = () -> {
            try {
                cache.lock(keys.get(1)).tryLock();
            }
            catch (Throwable e) {
                lockErr.set(e);
            }

            spi(backup).waitForBlocked();
            spi(backup).stopBlock();
        };

        stopGrid(0);

        assertTrue("Lock on stopping node must fail", X.hasCause(lockErr.get(), NodeStoppingException.class));
        assertNull("No failures", failureHnd.failureContext());
    }
}

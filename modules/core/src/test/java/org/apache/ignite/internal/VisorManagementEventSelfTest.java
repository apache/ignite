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

package org.apache.ignite.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.client.thin.TestTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_MANAGEMENT_TASK_STARTED;

/**
 *
 */
public class VisorManagementEventSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable visor management events.
        cfg.setIncludeEventTypes(
            EVT_MANAGEMENT_TASK_STARTED
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Current test case start valid one node visor task that has GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testManagementOneNodeVisorTask() throws Exception {
        doTestVisorTask(TestManagementVisorOneNodeTask.class, true);
    }

    /**
     * Current test case start valid multi node visor task that has GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testManagementMultiNodeVisorTask() throws Exception {
        doTestVisorTask(TestManagementVisorMultiNodeTask.class, true);
    }

    /**
     * Current test case start one node visor task that has not GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotManagementTask() throws Exception {
        doTestVisorTask(TestTask.class, false);
    }

    /**
     * @param cls class of the task.
     *
     * @throws Exception If failed.
     */
    private void doTestVisorTask(Class<? extends ComputeTask<?, ?>> cls, boolean expEvt) throws Exception {
        IgniteEx ignite = startGrid(0);

        String arg = "test-arg";

        final AtomicReference<TaskEvent> evt = new AtomicReference<>();

        final CountDownLatch evtLatch = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<TaskEvent>() {
            @Override public boolean apply(TaskEvent e) {
                evt.set(e);

                evtLatch.countDown();

                return false;
            }
        }, EventType.EVT_MANAGEMENT_TASK_STARTED);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            ignite.compute().executeAsync(cls.getName(), new VisorTaskArgument<>(node.id(), arg, true));

        if (expEvt) {
            assertTrue(evtLatch.await(10000, TimeUnit.MILLISECONDS));
            assertTrue(evt.get() instanceof TaskEvent);
        }
        else
            assertFalse(evtLatch.await(1000, TimeUnit.MILLISECONDS));
    }
}

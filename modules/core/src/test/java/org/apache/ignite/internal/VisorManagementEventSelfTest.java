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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.thin.TestTask;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_MANAGEMENT_TASK_STARTED;

/**
 *
 */
public class VisorManagementEventSelfTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Enable visor management events.
        cfg.setIncludeEventTypes(
            EVT_MANAGEMENT_TASK_STARTED
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testManagementTask() throws Exception {
        ignite.commandsRegistry().register(new TestComputeCommand());

        doTestManagementTask(TestManagementTask.class, true);
    }

    /** */
    @Test
    public void testManagementTaskLocalCommand() throws Exception {
        ignite.commandsRegistry().register(new TestLocalCommand());

        doTestManagementTask(TestManagementTask.class, true);
    }

    /** */
    @Test
    public void testNotManagementTask() throws Exception {
        doTestManagementTask(TestTask.class, false);
    }

    /**
     * @param cls class of the task.
     *
     * @throws Exception If failed.
     */
    private void doTestManagementTask(Class<? extends ComputeTask<?, ?>> cls, boolean expEvt) throws Exception {
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
            ignite.compute().executeAsync(cls.getName(), new VisorTaskArgument<>(node.id(), new TestCommandArg(), true));

        if (expEvt) {
            assertTrue(evtLatch.await(10000, TimeUnit.MILLISECONDS));
            assertTrue(evt.get() instanceof TaskEvent);
        }
        else
            assertFalse(evtLatch.await(1000, TimeUnit.MILLISECONDS));
    }

    /** */
    private static class TestManagementTask extends VisorOneNodeTask<TestCommandArg, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override protected TestJob job(TestCommandArg arg) {
            return new TestJob(arg, debug);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected Object reduce0(List<ComputeJobResult> results) {
            return null;
        }

        /** */
        private static class TestJob extends VisorJob<TestCommandArg, Object> {
            /** */
            private static final long serialVersionUID = 0L;

            /** */
            protected TestJob(TestCommandArg arg, boolean debug) {
                super(arg, debug);
            }

            /** {@inheritDoc} */
            @Override protected Object run(TestCommandArg arg) {
                return null;
            }
        }
    }

    /** */
    private static class TestLocalCommand implements LocalCommand<IgniteDataTransferObject, Object> {
        /** {@inheritDoc} */
        @Override public String description() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Class<IgniteDataTransferObject> argClass() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Class<? extends ComputeTask<?, ?>>[] taskClasses() {
            return F.asArray(TestManagementTask.class);
        }

        /** {@inheritDoc} */
        @Override public Object execute(
            @Nullable GridClient cli,
            @Nullable Ignite ignite,
            IgniteDataTransferObject arg,
            Consumer<String> printer
        ) throws GridClientException {
            return null;
        }
    }

    /** */
    private static class TestComputeCommand implements ComputeCommand<TestCommandArg, Object> {
        /** {@inheritDoc} */
        @Override public String description() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Class<TestCommandArg> argClass() {
            return TestCommandArg.class;
        }

        /** {@inheritDoc} */
        @Override public Class<? extends ComputeTask<VisorTaskArgument<TestCommandArg>, Object>> taskClass() {
            return TestManagementTask.class;
        }
    }

    /** */
    private static class TestCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) {
            // No-op.
        }
    }
}

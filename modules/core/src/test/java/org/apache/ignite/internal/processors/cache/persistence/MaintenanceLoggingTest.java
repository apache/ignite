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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.MAINT_MODE_BASIC_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.MAINT_MODE_ENABLED;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK0_ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK0_COMPLETE;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK1_ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK1_COMPLETE;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK1_REGISTERED;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK2_ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK2_CALLED;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK2_FAILED;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.TASK2_REGISTERED;
import static org.apache.ignite.internal.processors.cache.persistence.MaintenanceLoggingTest.Listeners.values;

/**
 * Tests for maintenance logging.
 */
public class MaintenanceLoggingTest extends GridCommonAbstractTest {
    /** */
    private static final String TASK_NAME = "taskName";

    /** */
    private static final String DESCRIPTION = "Custom maintenance task";

    /** */
    private static final String PARAMS = "any_param, param";

    /** */
    private static final String ACTION = "action";

    /** */
    private static final AtomicInteger ACTION_EXECUTED = new AtomicInteger();

    /** Listening test logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(10 * 1024 * 1024)
                                        .setPersistenceEnabled(true)
                        )
        );

        cfg.setMetricsLogFrequency(1000);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        Arrays.stream(values()).map(Listeners::lsnr).forEach(listeningLog::registerListener);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        listeningLog.clearListeners();

        super.afterTest();
    }

    /**
     * Test acknowledge Node Basic Metrics logging for information about the maintenance mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMaintenanceLogging() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertFalse(ignite.context().maintenanceRegistry().isMaintenanceMode());

        for (int i = 0; i < 3; i++) {
            ignite.context().maintenanceRegistry()
                    .registerMaintenanceTask(new MaintenanceTask(TASK_NAME + i, DESCRIPTION + i, PARAMS + i));
        }

        stopGrid(0);

        ignite = startGrid(0);

        assertTrue(ignite.context().maintenanceRegistry().isMaintenanceMode());

        for (int i = 0; i < 3; i++) {
            ignite.context().maintenanceRegistry()
                    .registerWorkflowCallback(TASK_NAME + i, new SimpleMaintenanceCallback(Arrays.asList(
                            new SimpleAction(ACTION + (i + i)),
                            new SimpleAction(ACTION + (i + i + 1)))));
        }

        try {
            ignite.context().maintenanceRegistry().prepareAndExecuteMaintenance();
        }
        catch (AssertionError ignore) {
            //do nothing
        }

        assertTrue(MAINT_MODE_ENABLED.lsnr().check());
        assertTrue(MAINT_MODE_BASIC_METRICS.lsnr().check());
        assertTrue(TASK0_ACTIVE.lsnr().check(60000));
        assertTrue(TASK0_COMPLETE.lsnr().check(60000));
        assertTrue(TASK1_REGISTERED.lsnr().check(60000));
        assertTrue(TASK1_ACTIVE.lsnr().check(60000));
        assertTrue(TASK1_COMPLETE.lsnr().check(60000));
        assertTrue(TASK2_REGISTERED.lsnr().check(60000));
        assertTrue(TASK2_ACTIVE.lsnr().check(60000));
        assertTrue(TASK2_FAILED.lsnr().check(60000));
        assertEquals(3, ACTION_EXECUTED.get());

        ignite.context().maintenanceRegistry().actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();

        assertTrue(TASK2_CALLED.lsnr().check(60000));
        assertEquals(4, ACTION_EXECUTED.get());

        ignite.context().maintenanceRegistry().actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();
    }

    /** */
    private final class SimpleMaintenanceCallback implements MaintenanceWorkflowCallback {
        /** */
        private final List<MaintenanceAction<?>> actions = new ArrayList<>();

        /** */
        SimpleMaintenanceCallback(List<MaintenanceAction<?>> actions) {
            this.actions.addAll(actions);
        }

        /** {@inheritDoc} */
        @Override public boolean shouldProceedWithMaintenance() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public @NotNull List<MaintenanceAction<?>> allActions() {
            return actions;
        }

        /** {@inheritDoc} */
        @Override public @Nullable MaintenanceAction<?> automaticAction() {
            return actions.get(0);
        }
    }

    /** */
    private class SimpleAction implements MaintenanceAction<Void> {
        /** */
        private final String name;

        /** */
        private SimpleAction(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public Void execute() {
            ACTION_EXECUTED.incrementAndGet();

            try {
                Thread.sleep(2000);

                if ((name).equals(ACTION + "4")) fail();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public @NotNull String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public @Nullable String description() {
            return null;
        }
    }

    /** */
    enum Listeners {
        /** */
        MAINT_MODE_ENABLED(LogListener.matches("ATTENTION! MAINTENANCE MODE ENABLED!").build()),

        /** */
        MAINT_MODE_BASIC_METRICS(LogListener.matches("!!! ATTENTION! Node is in Maintenance Mode !!!").build()),

        /** */
        TASK0_ACTIVE(LogListener.matches("^-- (ACTIVE    ) name=taskName0, description=Custom maintenance task0, " +
                "params=any_param, param0, currentAction=action0").build()),

        /** */
        TASK0_COMPLETE(LogListener.matches("^-- (COMPLETE  ) name=taskName0, description=Custom maintenance task0, " +
                "params=any_param, param0").build()),

        /** */
        TASK1_ACTIVE(LogListener.matches("^-- (ACTIVE    ) name=taskName1, description=Custom maintenance task1, " +
                "params=any_param, param1, currentAction=action2").build()),

        /** */
        TASK1_COMPLETE(LogListener.matches("^-- (COMPLETE  ) name=taskName1, description=Custom maintenance task1, " +
                "params=any_param, param1").build()),

        /** */
        TASK1_REGISTERED(LogListener.matches("^-- (REGISTERED) name=taskName1, description=Custom maintenance task1, " +
                "params=any_param, param1").build()),

        /** */
        TASK2_ACTIVE(LogListener.matches("^-- (ACTIVE    ) name=taskName2, description=Custom maintenance task2, " +
                "params=any_param, param2, currentAction=action4").build()),

        /** */
        TASK2_CALLED(LogListener.matches("^-- (CALLED    ) name=taskName2, description=Custom maintenance task2, " +
                "params=any_param, param2").build()),

        /** */
        TASK2_FAILED(LogListener.matches("^-- (FAILED    ) name=taskName2, description=Custom maintenance task2, " +
                "params=any_param, param2").build()),

        /** */
        TASK2_REGISTERED(LogListener.matches("^-- (REGISTERED) name=taskName2, description=Custom maintenance task2, " +
                "params=any_param, param2").build());

        /** */
        private final LogListener lsnr;

        /** */
        Listeners(LogListener lsnr) {
            this.lsnr = lsnr;
        }

        /** */
        private LogListener lsnr() {
            return lsnr;
        }
    }
}

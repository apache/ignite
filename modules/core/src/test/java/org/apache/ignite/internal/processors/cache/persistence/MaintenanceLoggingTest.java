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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexTarget;
import org.apache.ignite.internal.cache.query.index.sorted.maintenance.RebuildIndexWorkflowCallback;
import org.apache.ignite.internal.maintenance.MaintenanceTaskState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;
import static org.apache.ignite.internal.maintenance.MaintenanceTaskState.State;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

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

    /** */
    private static MaintenanceRegistry mntcReg;

    /** Listening test logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** */
    private Semaphore semaphore;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(100 * 1024 * 1024)
                        .setPersistenceEnabled(true)
        ));

        cfg.setMetricsLogFrequency(1000);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        Arrays.stream(Listeners.values()).map(Listeners::lsnr).forEach(lsnr -> {
            lsnr.reset();
            listeningLog.registerListener(lsnr);
        });

        ACTION_EXECUTED.set(0);

        semaphore = new Semaphore(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        listeningLog.clearListeners();

        ACTION_EXECUTED.set(0);

        super.afterTest();
    }

    /** */
    @Test
    public void testLogRegisterActiveCompleteUnregisterOneTask() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, null, false)))));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK_COMPLETED.lsnr().check(2000));
        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneTask() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, null, true)))));

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (RuntimeException ignored) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK_FAILED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCancelledUnregisterOneTask() throws Exception {
        SimpleAction startAction = new SimpleAction(ACTION + 2, semaphore, false);
        StopAction stopAction = new StopAction("stopAction", startAction);

        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                startAction,
                stopAction
            ))));

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME).get(1).execute();

        assertTrue(Listeners.TASK_CANCELLED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED.lsnr().check(2000));
    }

    /** */
    @Test
    public void testLogRegisteredUnregisterOneTaskShouldProceedWithMaintenanceFalse() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION, null, false)), false)));

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK_FAILED.lsnr().check(2000));
        assertEquals(0, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCompleteUnregisterOneTaskTwoActions() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME + 2, DESCRIPTION + 2, PARAMS + 2),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 4, null, false, false),
                new SimpleAction(ACTION + 5, null, false)
            ))));

        assertTrue(Listeners.TASK2_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK2_ACTIVE1.lsnr().check(2000));
        assertTrue(Listeners.TASK2_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK2_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();

        assertTrue(Listeners.TASK2_ACTIVE2.lsnr().check(2000));
        assertTrue(Listeners.TASK2_SECOND_COMPLETED.lsnr().check(2000));
        assertTrue(Listeners.TASK2_UNREGISTERED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCompleteUnregisterOneTaskDaemonThread() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, semaphore, false, true)
            ))));

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK_UNREGISTERED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveFailedOneTaskDaemonThread() throws Exception {
        prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, semaphore, true)
            ))));

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_FAILED.lsnr().check(2000));
        assertFalse(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveCompleteUnregisterTwoTasks() throws Exception {
        prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 0, null, false))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 2, null, false)))
        ));

        assertTrue(Listeners.TASK0_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK0_COMPLETED.lsnr().check(2000));
        assertTrue(Listeners.TASK0_UNREGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK1_COMPLETED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_UNREGISTERED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneOfTwoTasks() throws Exception {
        prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 0, null, true))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 2, null, false)))
        ));

        assertTrue(Listeners.TASK0_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (Exception ignored) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK0_FAILED.lsnr().check(2000));
        assertFalse(Listeners.TASK0_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertFalse(Listeners.TASK1_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK1_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneOfTwoTasksDaemonThread() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);

        prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 0, semaphore, true))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION + 2, semaphore1, false)))
        ));

        assertTrue(Listeners.TASK0_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (Exception ignored) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK1_ACTIVE.lsnr().check(2000));

        semaphore.release();
        semaphore1.release();

        assertTrue(Listeners.TASK0_FAILED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_COMPLETED.lsnr().check(2000));

        semaphore1.release();

        assertTrue(Listeners.TASK1_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK0_UNREGISTERED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogUnregisteredTask() throws Exception {
        prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION, null, true)))
        ));

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.unregisterMaintenanceTask(TASK_NAME);

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertEquals(0, ACTION_EXECUTED.get());
    }

    /**
     * Test acknowledge Node Basic Metrics logging for information about the maintenance mode with different tasks.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedTaskLog() throws Exception {
        Map<MaintenanceTask, MaintenanceWorkflowCallback> tasks = new LinkedHashMap<>();

        for (int i = 0; i < 3; i++) {
            tasks.put(new MaintenanceTask(TASK_NAME + i, DESCRIPTION + i, PARAMS + i),
                new SimpleMaintenanceCallback(Arrays.asList(
                    new SimpleAction(ACTION + (i + i), null, (i + i) == 2),
                    new SimpleAction(ACTION + (i + i + 1), semaphore, (i + i + 1) == 5)))
            );
        }

        prepareMaintenance(tasks);

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (Exception ignore) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.MAINT_MODE_ENABLED.lsnr().check());
        assertTrue(Listeners.MAINT_MODE_BASIC_METRICS.lsnr().check());
        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK0_COMPLETED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK1_FAILED.lsnr().check(2000));
        assertTrue(Listeners.TASK2_REGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK2_ACTIVE1.lsnr().check(2000));
        assertFalse(Listeners.TASK2_COMPLETED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();

        assertTrue(Listeners.TASK2_ACTIVE2.lsnr().check(2000));
        semaphore.release();
        assertTrue(Listeners.TASK2_FAILED.lsnr().check(2000));
        assertEquals(3, ACTION_EXECUTED.get());

        assertThrowsAnyCause(
            listeningLog,
            () -> mntcReg.actionsForMaintenanceTask(TASK_NAME + 0).get(1).execute(),
            IgniteException.class,
            "Maintenance workflow callback for given task name not found, " +
                "cannot retrieve maintenance actions for it: taskName0"
        );

        MaintenanceWorkflowCallback callback = new SimpleMaintenanceCallback(Arrays.asList(
            new SimpleAction(ACTION + "_A", semaphore, false),
            new SimpleAction(ACTION + "_B", semaphore, false)));

        mntcReg.registerWorkflowCallback(TASK_NAME, callback);

        assertTrue(Listeners.TASK_NOT_REGISTERED.lsnr().check(2000));

        callback.allActions().get(0).execute();

        assertTrue(Listeners.TASK_NOT_REGISTERED_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK_NOT_REGISTERED_COMPLETED.lsnr().check(2000));
        assertEquals(4, ACTION_EXECUTED.get());

        assertThrowsAnyCause(
            listeningLog,
            () -> mntcReg.actionsForMaintenanceTask(TASK_NAME).get(1).execute(),
            IgniteException.class,
            "Maintenance workflow callback for given task name not found, " +
                "cannot retrieve maintenance actions for it: taskName"
        );
    }

    /** */
    @Test
    public void testCorruptedPdsMaintenanceCallbackActionsLog() throws Exception {
        IgniteEx ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertFalse(mntcReg.isMaintenanceMode());

        MaintenanceTask mntcTask = new MaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME + 1,
            "Corrupted cache groups found", DEFAULT_CACHE_NAME);

        mntcReg = ignite.context().maintenanceRegistry();

        mntcReg.registerMaintenanceTask(mntcTask);

        stopGrid(0);

        ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertTrue(mntcReg.isMaintenanceMode());

        mntcReg.registerWorkflowCallback(CORRUPTED_DATA_FILES_MNTC_TASK_NAME + 1,
            new CorruptedPdsMaintenanceCallback(ignite.context().pdsFolderResolver().fileTree(),
            Arrays.asList(mntcTask.parameters().split(Pattern.quote(File.separator)))));

        mntcReg.actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME + 1).stream()
            .filter(a -> a.name().equals(CheckCorruptedCacheStoresCleanAction.ACTION_NAME))
            .findFirst().get().execute();

        assertTrue(Listeners.CHECK_CORRUPTED.lsnr().check(5000));

        mntcReg.actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME + 1).stream()
            .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME))
            .findFirst().get().execute();

        assertTrue(Listeners.CLEAN_CACHE.lsnr().check(5000));
    }

    /** */
    @Test
    public void testRebuildIndexActionCompletedLog() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setBackups(1));

        try (IgniteClient client = Ignition.startClient(
            new ClientConfiguration()
                .setAddresses(ignite.localNode().addresses().iterator().next())
                .setPartitionAwarenessEnabled(true)
                .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true))
        )) {

            client.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS TEST_TABLE (name VARCHAR PRIMARY KEY, text VARCHAR) " +
                "WITH \"CACHE_NAME=" + DEFAULT_CACHE_NAME + "\"").setSchema("PUBLIC").setTimeout(30, TimeUnit.SECONDS));

            client.query(new SqlFieldsQuery(String.format("CREATE INDEX IF NOT EXISTS %s ON %s", "idxName", "TEST_TABLE")));

            for (int i = 0; i < 2048; i++) {
                try {
                    client.query(new SqlFieldsQuery("INSERT INTO PUBLIC.TEST_TABLE VALUES('Name_" + i + "', 'Text_" + i + "')"));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        forceCheckpoint(ignite);

        MaintenanceTask mntcTask = toMaintenanceTask(CU.cacheId(DEFAULT_CACHE_NAME), "idxName");

        ignite.context().maintenanceRegistry().registerMaintenanceTask(mntcTask);

        stopGrid(0);

        ignite = startGrid(0);

        assertTrue(ignite.context().maintenanceRegistry().isMaintenanceMode());

        assertTrue(Listeners.INDEX_REBUILD_COMPLETED.lsnr().check(5000));
    }

    /** */
    @Test
    public void testRebuildIndexActionFailedLog() throws Exception {
        IgniteEx ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertFalse(mntcReg.isMaintenanceMode());

        MaintenanceTask mntcTask = new MaintenanceTask("indexRebuildMaintenanceLogTask", "Corrupted index found", DEFAULT_CACHE_NAME);

        mntcReg = ignite.context().maintenanceRegistry();

        mntcReg.registerMaintenanceTask(mntcTask);

        stopGrid(0);

        ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertTrue(mntcReg.isMaintenanceMode());

        mntcReg.registerWorkflowCallback("indexRebuildMaintenanceLogTask",
            new RebuildIndexWorkflowCallback(List.of(new MaintenanceRebuildIndexTarget(0, "idxName")), ignite.context()));

        mntcReg.actionsForMaintenanceTask("indexRebuildMaintenanceLogTask").get(0).execute();

        assertTrue(Listeners.INDEX_REBUILD_FAILED.lsnr().check(5000));
    }

    /** */
    private static final class SimpleMaintenanceCallback implements MaintenanceWorkflowCallback {
        /** */
        private final List<MaintenanceAction<?>> actions = new ArrayList<>();

        /** */
        private final Boolean shouldProceedWithMaintenance;

        /** */
        SimpleMaintenanceCallback(List<MaintenanceAction<?>> actions) {
            this.actions.addAll(actions);
            shouldProceedWithMaintenance = true;
        }

        /** */
        SimpleMaintenanceCallback(List<MaintenanceAction<?>> actions, Boolean shouldProceedWithMaintenance) {
            this.actions.addAll(actions);
            this.shouldProceedWithMaintenance = shouldProceedWithMaintenance;
        }

        /** {@inheritDoc} */
        @Override public boolean shouldProceedWithMaintenance() {
            return shouldProceedWithMaintenance;
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
    private static class SimpleAction implements MaintenanceAction<Void> {
        /** */
        private final String name;

        /** */
        private volatile MaintenanceTaskState state;

        /** */
        private final Semaphore semaphore;

        /** */
        private final boolean isFailed;

        /** */
        private final boolean unregister;

        /** */
        private volatile Thread worker;

        /** */
        private SimpleAction(String name, Semaphore semaphore, Boolean isFailed, Boolean unregister) {
            this.name = name;
            this.semaphore = semaphore;
            this.isFailed = isFailed;
            this.unregister = unregister;
        }

        /** */
        private SimpleAction(String name, Semaphore semaphore, Boolean isFailed) {
            this(name, semaphore, isFailed, true);
        }

        /** {@inheritDoc} */
        @Override public Void execute() {
            state.currAction(name).state(State.ACTIVE);

            ACTION_EXECUTED.incrementAndGet();

            if (semaphore == null) {
                execute0();
            }
            else {
                worker = new Thread(this::execute0);

                worker.setDaemon(true);

                worker.start();
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

        /** {@inheritDoc} */
        @Override public void state(@NotNull MaintenanceTaskState state) {
            this.state = state;
        }

        /** */
        private void execute0() {
            InterruptibleAction pause = semaphore == null
                ? () -> Thread.sleep(2000)
                : semaphore::acquire;

            try {
                pause.execute();

                if (isFailed) throw new RuntimeException("Action " + name + " failed");

                state.state(State.COMPLETED);

                pause.execute();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                return;
            }
            catch (Exception e) {
                state.state(State.FAILED);

                throw new RuntimeException(e);
            }

            if (unregister) mntcReg.unregisterMaintenanceTask(state.task().name());
        }

        /**
         * @return {@code true} if action was cancelled.
         */
        public synchronized Boolean cancel() {
            Thread thread = worker;

            if (thread == null || !thread.isAlive())
                return false;

            try {
                thread.interrupt();

                thread.join(10_000);

                return !thread.isAlive();
            }
            catch (Exception e) {
                return false;
            }
        }
    }

    /** */
    private static class StopAction implements MaintenanceAction<Void> {
        /** */
        private final String name;

        /** */
        private volatile MaintenanceTaskState state;

        /** */
        private final SimpleAction startAction;

        /** */
        private StopAction(String name, SimpleAction startAction) {
            this.name = name;
            this.startAction = startAction;
        }

        /** {@inheritDoc} */
        @Override public Void execute() {
            try {
                if (startAction.cancel()) {
                    state.currAction(name).state(State.CANCELLED);

                    assertTrue(Listeners.TASK_CANCELLED.lsnr().check(2000));

                    mntcReg.unregisterMaintenanceTask(state.task().name());
                }
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

        /** {@inheritDoc} */
        @Override public void state(@NotNull MaintenanceTaskState state) {
            this.state = state;
        }
    }

    /** */
    @FunctionalInterface
    interface InterruptibleAction {
        /** */
        void execute() throws InterruptedException;
    }

    /** */
    private void prepareMaintenance(Map<MaintenanceTask, MaintenanceWorkflowCallback> tasks) throws Exception {
        IgniteEx ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertFalse(mntcReg.isMaintenanceMode());

        for (MaintenanceTask task : tasks.keySet()) {
            mntcReg.registerMaintenanceTask(task);
        }

        stopGrid(0);

        ignite = startGrid(0);

        mntcReg = ignite.context().maintenanceRegistry();

        assertTrue(mntcReg.isMaintenanceMode());

        for (Map.Entry<MaintenanceTask, MaintenanceWorkflowCallback> task : tasks.entrySet()) {
            mntcReg.registerWorkflowCallback(task.getKey().name(), task.getValue());
        }
    }

    /** */
    enum Listeners {
        /** */
        CHECK_CORRUPTED(LogListener.matches("(REGISTERED->ACTIVE->COMPLETED) name=corrupted-cache-data-files-task1, " +
            "description=Corrupted cache groups found, params=").build()),

        /** */
        CLEAN_CACHE(LogListener.matches("(REGISTERED->ACTIVE->COMPLETED->ACTIVE->COMPLETED) " +
            "name=corrupted-cache-data-files-task1, description=Corrupted cache groups found, params=").build()),

        /** */
        INDEX_REBUILD_COMPLETED(LogListener.matches("(REGISTERED->ACTIVE->UNREGISTERED->COMPLETED) " +
            "name=indexRebuildMaintenanceTask, description=Corrupted index found, params=").build()),

        /** */
        INDEX_REBUILD_FAILED(LogListener.matches("(REGISTERED->ACTIVE->FAILED) name=indexRebuildMaintenanceLogTask, " +
            "description=Corrupted index found, params=").build()),

        /** */
        MAINT_MODE_BASIC_METRICS(LogListener.matches("!!! ATTENTION! NODE IS IN MAINTENANCE MODE !!!").build()),

        /** */
        MAINT_MODE_ENABLED(LogListener.matches("ATTENTION! MAINTENANCE MODE ENABLED!").build()),

        /** */
        TASK_ACTIVE(LogListener.matches("ACTIVE) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param], currentAction=action").build()),

        /** */
        TASK_CANCELLED(LogListener.matches("CANCELLED) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param]").build()),

        /** */
        TASK_COMPLETED(LogListener.matches("COMPLETED) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param]").build()),

        /** */
        TASK_FAILED(LogListener.matches("FAILED) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param]").build()),

        /** */
        TASK_NOT_REGISTERED(LogListener.matches("NOT_REGISTERED) name=taskName").build()),

        /** */
        TASK_NOT_REGISTERED_ACTIVE(LogListener.matches("ACTIVE) name=taskName, currentAction=action").build()),

        /** */
        TASK_NOT_REGISTERED_COMPLETED(LogListener.matches("ACTIVE->COMPLETED) name=taskName").build()),

        /** */
        TASK_REGISTERED(LogListener.matches("^-- (REGISTERED) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param]").build()),

        /** */
        TASK_UNREGISTERED(LogListener.matches("UNREGISTERED) name=taskName, description=Custom maintenance task, " +
            "params=[any_param, param]").build()),

        /** */
        TASK0_REGISTERED(LogListener.matches("(REGISTERED) name=taskName0, description=Custom maintenance task0, " +
            "params=[any_param, param0]").build()),

        /** */
        TASK0_ACTIVE(LogListener.matches("ACTIVE) name=taskName0, description=Custom maintenance task0, " +
            "params=[any_param, param0], currentAction=action0").build()),

        /** */
        TASK0_COMPLETED(LogListener.matches("ACTIVE->COMPLETED) name=taskName0, description=Custom maintenance task0, " +
            "params=[any_param, param0]").build()),

        /** */
        TASK0_FAILED(LogListener.matches("FAILED) name=taskName0, description=Custom maintenance task0, " +
            "params=[any_param, param0]").build()),

        /** */
        TASK0_UNREGISTERED(LogListener.matches("UNREGISTERED) name=taskName0, description=Custom maintenance task0, " +
            "params=[any_param, param0]").build()),

        /** */
        TASK1_ACTIVE(LogListener.matches("ACTIVE) name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1], currentAction=action2").build()),

        /** */
        TASK1_COMPLETED(LogListener.matches("ACTIVE->COMPLETED) name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1]").build()),

        /** */
        TASK1_FAILED(LogListener.matches("FAILED) name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1]").build()),

        /** */
        TASK1_REGISTERED(LogListener.matches("^-- (REGISTERED) name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1]").build()),

        /** */
        TASK1_UNREGISTERED(LogListener.matches("UNREGISTERED) name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1]").build()),

        /** */
        TASK2_ACTIVE1(LogListener.matches("ACTIVE) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2], currentAction=action4").build()),

        /** */
        TASK2_ACTIVE2(LogListener.matches("ACTIVE) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2], currentAction=action5").build()),

        /** */
        TASK2_COMPLETED(LogListener.matches("ACTIVE->COMPLETED) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2]").build()),

        /** */
        TASK2_SECOND_COMPLETED(LogListener.matches("(REGISTERED->ACTIVE->COMPLETED->ACTIVE->COMPLETED->UNREGISTERED) " +
            "name=taskName2, description=Custom maintenance task2, params=[any_param, param2]").build()),

        /** */
        TASK2_FAILED(LogListener.matches("FAILED) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2]").build()),

        /** */
        TASK2_REGISTERED(LogListener.matches("^-- (REGISTERED) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2]").build()),

        /** */
        TASK2_UNREGISTERED(LogListener.matches("UNREGISTERED) name=taskName2, description=Custom maintenance task2, " +
            "params=[any_param, param2]").build());

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

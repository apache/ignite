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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.maintenance.AsyncActionStateCallback;
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
import org.junit.Test;

import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;
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

    /** Listening test logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

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
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, TASK_NAME, null, false, registryRef::get)))));

        registryRef.set(mntcReg);

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE1.lsnr().check(2000));
        assertTrue(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneTask() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, TASK_NAME, null, true, registryRef::get)))));

        registryRef.set(mntcReg);

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (RuntimeException ignored) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.TASK_ACTIVE1.lsnr().check(2000));
        assertTrue(Listeners.TASK_FAILED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCancelledUnregisterOneTask() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        SimpleAction startAction = new AsyncAction(ACTION + 2, TASK_NAME, semaphore, false, registryRef::get);
        StopAction stopAction = new StopAction("stopAction", startAction);

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                startAction,
                stopAction
            ))));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE2.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME).stream()
            .filter(action -> "stopAction".equals(action.name())).findFirst().ifPresent(MaintenanceAction::execute);

        assertTrue(Listeners.TASK_CANCELLED.lsnr().check(2000));

        mntcReg.unregisterMaintenanceTask(TASK_NAME);

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));
    }

    /** */
    @Test
    public void testLogRegisteredActiveFailedStopOneTask() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        SimpleAction startAction = new AsyncAction(ACTION + 2, TASK_NAME, semaphore, false, registryRef::get);
        StopAction stopAction = new StopAction("failedStopAction", startAction);

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                startAction,
                stopAction
            ))));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE2.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME).stream().filter(action -> "failedStopAction".equals(action.name()))
            .findFirst().ifPresent(MaintenanceAction::execute);

        assertTrue(Listeners.TASK_STOP_FAILED.lsnr().check(2000));
    }

    /** */
    @Test
    public void testLogRegisteredUnregisterOneTaskShouldProceedWithMaintenanceFalse() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(new SimpleAction(ACTION, TASK_NAME, null, false, registryRef::get)), false)));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_ACTIVE1.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_FAILED.lsnr().check(2000));
        assertEquals(0, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCompleteUnregisterOneTaskTwoActions() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME + 2, DESCRIPTION + 2, PARAMS + 2),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 4, TASK_NAME + 2, null, false, null),
                new SimpleAction(ACTION + 5, TASK_NAME + 2, null, false, registryRef::get)
            ))));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK2_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK2_ACTIVE1.lsnr().check(2000));
        assertTrue(Listeners.TASK2_COMPLETED_ONE_ACTION.lsnr().check(2000));
        assertFalse(Listeners.TASK2_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();

        assertTrue(Listeners.TASK2_ACTIVE2.lsnr().check(2000));
        assertTrue(Listeners.TASK2_COMPLETED_TWO_ACTIONS.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveCompleteUnregisterOneTaskDaemonThread() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new AsyncAction(ACTION, TASK_NAME, semaphore, false, registryRef::get)
            ))));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE1.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisteredActiveFailedOneTaskDaemonThread() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new AsyncAction(ACTION, TASK_NAME, semaphore, true, registryRef::get)
            ))));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK_ACTIVE1.lsnr().check(2000));
        assertFalse(Listeners.TASK_COMPLETED_UNREGISTERED.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK_FAILED.lsnr().check(2000));
        assertFalse(Listeners.TASK_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveCompleteUnregisterTwoTasks() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 0, TASK_NAME + 0, null, false, registryRef::get))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 2, TASK_NAME + 1, null, false, registryRef::get)))
        ));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK0_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));

        mntcReg.prepareAndExecuteMaintenance();

        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK0_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK1_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneOfTwoTasks() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 0, TASK_NAME + 0, null, true, registryRef::get))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION + 2, TASK_NAME + 1, null, false, registryRef::get
            )))
        ));

        registryRef.set(mntcReg);

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
        assertFalse(Listeners.TASK0_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertFalse(Listeners.TASK1_COMPLETED.lsnr().check(2000));
        assertFalse(Listeners.TASK1_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertEquals(1, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogRegisterActiveFailedOneOfTwoTasksDaemonThread() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME + 0, DESCRIPTION + 0, PARAMS + 0),
            new SimpleMaintenanceCallback(List.of(
                new AsyncAction(ACTION + 0, TASK_NAME + 0, semaphore, true, registryRef::get))),
            new MaintenanceTask(TASK_NAME + 1, DESCRIPTION + 1, PARAMS + 1),
            new SimpleMaintenanceCallback(List.of(
                new AsyncAction(ACTION + 2, TASK_NAME + 1, semaphore1, false, registryRef::get)))
        ));

        registryRef.set(mntcReg);

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

        assertTrue(Listeners.TASK1_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK0_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());
    }

    /** */
    @Test
    public void testLogUnregisteredTask() throws Exception {
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();

        MaintenanceRegistry mntcReg = prepareMaintenance(Map.of(
            new MaintenanceTask(TASK_NAME, DESCRIPTION, PARAMS),
            new SimpleMaintenanceCallback(List.of(
                new SimpleAction(ACTION, TASK_NAME, null, true, registryRef::get)))
        ));

        registryRef.set(mntcReg);

        assertTrue(Listeners.TASK_REGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK_UNREGISTERED.lsnr().check(2000));

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
        AtomicReference<MaintenanceRegistry> registryRef = new AtomicReference<>();
        Map<MaintenanceTask, MaintenanceWorkflowCallback> tasks = new LinkedHashMap<>();

        for (int i = 0; i < 3; i++) {
            tasks.put(new MaintenanceTask(TASK_NAME + i, DESCRIPTION + i, PARAMS + i),
                new SimpleMaintenanceCallback(Arrays.asList(
                    new SimpleAction(ACTION + (i + i), TASK_NAME + i, null, (i + i) == 2, (i + i) == 4 ? null : registryRef::get),
                    new AsyncAction(ACTION + (i + i + 1), TASK_NAME + i, semaphore, (i + i + 1) == 5, registryRef::get)))
            );
        }

        MaintenanceRegistry mntcReg = prepareMaintenance(tasks);

        registryRef.set(mntcReg);

        try {
            mntcReg.prepareAndExecuteMaintenance();
        }
        catch (Exception ignore) {
            // Ignore exception: maintenance action fails expected.
        }

        assertTrue(Listeners.MAINT_MODE_ENABLED.lsnr().check());
        assertTrue(Listeners.MAINT_MODE_BASIC_METRICS.lsnr().check());
        assertTrue(Listeners.TASK0_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK0_COMPLETED_UNREGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_REGISTERED.lsnr().check(2000));
        assertTrue(Listeners.TASK1_ACTIVE.lsnr().check(2000));
        assertTrue(Listeners.TASK1_FAILED.lsnr().check(2000));
        assertTrue(Listeners.TASK2_REGISTERED.lsnr().check(2000));
        assertFalse(Listeners.TASK2_ACTIVE1.lsnr().check(2000));
        assertFalse(Listeners.TASK2_COMPLETED_ONE_ACTION.lsnr().check(2000));
        assertEquals(2, ACTION_EXECUTED.get());

        mntcReg.actionsForMaintenanceTask(TASK_NAME + 2).get(0).execute();

        assertTrue(Listeners.TASK2_ACTIVE1.lsnr().check(2000));
        assertTrue(Listeners.TASK2_COMPLETED_ONE_ACTION.lsnr().check(2000));

        mntcReg.actionsForMaintenanceTask(TASK_NAME + 2).get(1).execute();

        assertTrue(Listeners.TASK2_ACTIVE2.lsnr().check(2000));

        semaphore.release();

        assertTrue(Listeners.TASK2_SECOND_ACTION_FAILED.lsnr().check(2000));
        assertEquals(4, ACTION_EXECUTED.get());

        assertThrowsAnyCause(
            listeningLog,
            () -> mntcReg.actionsForMaintenanceTask(TASK_NAME + 0).get(1).execute(),
            IgniteException.class,
            "Maintenance workflow callback for given task name not found, " +
                "cannot retrieve maintenance actions for it: taskName0"
        );
    }

    /** */
    @Test
    public void testCorruptedPdsMaintenanceCallbackActionsLog() throws Exception {
        IgniteEx ignite = startGrid(0);

        MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

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

        assertTrue(Listeners.CHECK_CACHE_FILES_CLEANED.lsnr().check(2000));

        mntcReg.actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME + 1).stream()
            .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME))
            .findFirst().get().execute();

        assertTrue(Listeners.CLEAN_CACHE.lsnr().check(2000));
    }

    /** */
    @Test
    public void testRebuildIndexActionFailedLog() throws Exception {
        IgniteEx ignite = startGrid(0);

        MaintenanceTask mntcTask = toMaintenanceTask(CU.cacheId(DEFAULT_CACHE_NAME), "idxName");

        ignite.context().maintenanceRegistry().registerMaintenanceTask(mntcTask);

        stopGrid(0);

        ignite = startGrid(0);

        assertTrue(ignite.context().maintenanceRegistry().isMaintenanceMode());
        assertTrue(Listeners.INDEX_REBUILD_FAILED.lsnr().check(2000));
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
        private final String taskName;

        /** */
        private final Semaphore semaphore;

        /** */
        private final boolean isFailed;

        /** */
        private final Supplier<MaintenanceRegistry> registrySupplier;

        /** */
        private volatile Thread worker;

        /** */
        protected volatile Runnable onCompleted = () -> {};

        /** */
        protected volatile Runnable onCancelled = () -> {};

        /** */
        protected volatile Consumer<Throwable> onFailed = e -> {};

        /** */
        private SimpleAction(String name, String taskName, Semaphore semaphore, Boolean isFailed,
                             Supplier<MaintenanceRegistry> registrySupplier) {
            this.name = name;
            this.taskName = taskName;
            this.semaphore = semaphore;
            this.isFailed = isFailed;
            this.registrySupplier = registrySupplier;
        }

        /** {@inheritDoc} */
        @Override public Void execute() {
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

        /** */
        private void execute0() {
            InterruptibleAction pause = semaphore == null
                ? () -> Thread.sleep(2000)
                : semaphore::acquire;

            try {
                pause.execute();

                if (isFailed)
                    throw new RuntimeException("Action " + name + " failed");

                onCompleted.run();

                pause.execute();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (Thread.currentThread().isAlive())
                    onCancelled.run();

                return;
            }
            catch (Exception e) {
                onFailed.accept(e);

                throw new RuntimeException(e);
            }

            if (registrySupplier != null && registrySupplier.get() != null) {
                registrySupplier.get().unregisterMaintenanceTask(taskName);
            }
        }

        /** @return {@code true} if action was cancelled. */
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
    private static final class AsyncAction extends SimpleAction implements AsyncActionStateCallback {
        /** */
        private AsyncAction(String name, String taskName, Semaphore semaphore, Boolean isFailed,
                            Supplier<MaintenanceRegistry> registrySupplier) {
            super(name, taskName, semaphore, isFailed, registrySupplier);
        }

        /** {@inheritDoc} */
        @Override public void setStateCallbacks(@NotNull Runnable onCompleted,
                                                @NotNull Runnable onCancelled,
                                                @NotNull Consumer<Throwable> onFailed) {
            this.onCompleted = onCompleted;
            this.onCancelled = onCancelled;
            this.onFailed = onFailed;
        }
    }

    /** */
    private static class StopAction implements MaintenanceAction<Boolean> {
        /** */
        private final String name;

        /** */
        private final SimpleAction startAction;

        /** */
        private StopAction(String name, SimpleAction startAction) {
            this.name = name;
            this.startAction = startAction;
        }

        /** {@inheritDoc} */
        @Override public Boolean execute() {
            try {
                if ("stopAction".equals(name) && startAction.cancel()) {
                    return true;
                }
            }
            catch (Exception e) {
                return false;
            }

            return false;
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
    @FunctionalInterface
    interface InterruptibleAction {
        /** */
        void execute() throws InterruptedException;
    }

    /** */
    private MaintenanceRegistry prepareMaintenance(Map<MaintenanceTask, MaintenanceWorkflowCallback> tasks) throws Exception {
        IgniteEx ignite = startGrid(0);

        MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

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

        return mntcReg;
    }

    /** */
    enum Listeners {
        /** */
        CHECK_CACHE_FILES_CLEANED(LogListener.matches("^-- REGISTERED name=corrupted-cache-data-files-task1, " +
                "description=Corrupted cache groups found, params=[default], " +
                "executedActions=[check_cache_files_cleaned=ACTIVE->COMPLETED]").build()),

        /** */
        CLEAN_CACHE(LogListener.matches("^-- REGISTERED name=corrupted-cache-data-files-task1, " +
                "description=Corrupted cache groups found, params=[default], " +
                "executedActions=[check_cache_files_cleaned=ACTIVE->COMPLETED, " +
                "clean_data_files=ACTIVE->COMPLETED]").build()),

        /** */
        INDEX_REBUILD_FAILED(LogListener.matches("^-- REGISTERED name=indexRebuildMaintenanceTask, " +
                "description=Corrupted index found, params=[1544803905|aWR4TmFtZQ==], " +
                "executedActions=[rebuild=ACTIVE->FAILED]").build()),

        /** */
        MAINT_MODE_BASIC_METRICS(LogListener.matches("!!! ATTENTION! NODE IS IN MAINTENANCE MODE !!!").build()),

        /** */
        MAINT_MODE_ENABLED(LogListener.matches("ATTENTION! MAINTENANCE MODE ENABLED!").build()),

        /** */
        TASK_ACTIVE1(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action=ACTIVE]").build()),

        /** */
        TASK_ACTIVE2(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action2=ACTIVE]").build()),

        /** */
        TASK_CANCELLED(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action2=ACTIVE->CANCELLED, " +
                "stopAction=ACTIVE->COMPLETED]").build()),

        /** */
        TASK_COMPLETED(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action=ACTIVE->COMPLETED]").build()),

        /** */
        TASK_COMPLETED_UNREGISTERED(LogListener.matches("^-- UNREGISTERED name=taskName, " +
                "description=Custom maintenance task, params=[any_param, param], " +
                "executedActions=[action=ACTIVE->COMPLETED]").build()),

        /** */
        TASK_FAILED(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action=ACTIVE->FAILED]").build()),

        /** */
        TASK_NOT_REGISTERED_ACTIVE(LogListener.matches("(REGISTERED->ACTIVE) name=taskName, currentAction=action").build()),

        /** */
        TASK_NOT_REGISTERED_COMPLETED(LogListener.matches("(REGISTERED->ACTIVE->COMPLETED) name=taskName").build()),

        /** */
        TASK_REGISTERED(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param]").build()),

        /** */
        TASK_STOP_FAILED(LogListener.matches("^-- REGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param], executedActions=[action2=ACTIVE, failedStopAction=ACTIVE->FAILED]").build()),

        /** */
        TASK_UNREGISTERED(LogListener.matches("^-- UNREGISTERED name=taskName, description=Custom maintenance task, " +
                "params=[any_param, param]").build()),

        /** */
        TASK0_ACTIVE(LogListener.matches("^-- REGISTERED name=taskName0, description=Custom maintenance task0, " +
                "params=[any_param, param0], executedActions=[action0=ACTIVE]").build()),

        /** */
        TASK0_COMPLETED_UNREGISTERED(LogListener.matches("^-- UNREGISTERED name=taskName0, " +
                "description=Custom maintenance task0, params=[any_param, param0], " +
                "executedActions=[action0=ACTIVE->COMPLETED]").build()),

        /** */
        TASK0_FAILED(LogListener.matches("^-- REGISTERED name=taskName0, description=Custom maintenance task0, " +
                "params=[any_param, param0], executedActions=[action0=ACTIVE->FAILED]").build()),

        /** */
        TASK0_REGISTERED(LogListener.matches("^-- REGISTERED name=taskName0, description=Custom maintenance task0, " +
                "params=[any_param, param0]").build()),

        /** */
        TASK1_ACTIVE(LogListener.matches("^-- REGISTERED name=taskName1, description=Custom maintenance task1, " +
                "params=[any_param, param1], executedActions=[action2=ACTIVE]").build()),

        /** */
        TASK1_COMPLETED(LogListener.matches("^-- REGISTERED name=taskName1, description=Custom maintenance task1, " +
                "params=[any_param, param1], executedActions=[action2=ACTIVE]").build()),

        /** */
        TASK1_COMPLETED_UNREGISTERED(LogListener.matches("^-- UNREGISTERED name=taskName1, " +
                "description=Custom maintenance task1, params=[any_param, param1], " +
                "executedActions=[action2=ACTIVE->COMPLETED]").build()),

        /** */
        TASK1_FAILED(LogListener.matches("^-- REGISTERED name=taskName1, description=Custom maintenance task1, " +
            "params=[any_param, param1], executedActions=[action2=ACTIVE->FAILED]").build()),

        /** */
        TASK1_REGISTERED(LogListener.matches("^-- REGISTERED name=taskName1, description=Custom maintenance task1, " +
                "params=[any_param, param1]").build()),

        /** */
        TASK2_ACTIVE1(LogListener.matches("^-- REGISTERED name=taskName2, description=Custom maintenance task2, " +
                "params=[any_param, param2], executedActions=[action4=ACTIVE]").build()),

        /** */
        TASK2_ACTIVE2(LogListener.matches("^-- REGISTERED name=taskName2, description=Custom maintenance task2, " +
                "params=[any_param, param2], executedActions=[action4=ACTIVE->COMPLETED, action5=ACTIVE]").build()),

        /** */
        TASK2_COMPLETED_ONE_ACTION(LogListener.matches("^-- REGISTERED name=taskName2, description=Custom maintenance task2, " +
                "params=[any_param, param2], executedActions=[action4=ACTIVE->COMPLETED]").build()),

        /** */
        TASK2_COMPLETED_TWO_ACTIONS(LogListener.matches("^-- UNREGISTERED name=taskName2, " +
                "description=Custom maintenance task2, params=[any_param, param2], " +
                "executedActions=[action4=ACTIVE->COMPLETED, action5=ACTIVE->COMPLETED]").build()),

        /** */
        TASK2_REGISTERED(LogListener.matches("^-- REGISTERED name=taskName2, description=Custom maintenance task2, " +
                "params=[any_param, param2]").build()),

        /** */
        TASK2_SECOND_ACTION_FAILED(LogListener.matches("^-- REGISTERED name=taskName2, " +
                "description=Custom maintenance task2, params=[any_param, param2], " +
                "executedActions=[action4=ACTIVE->COMPLETED, action5=ACTIVE->FAILED]").build()),

        /** */
        TASK2_UNREGISTERED(LogListener.matches("^-- UNREGISTERED name=taskName2, description=Custom maintenance task2, " +
                "params=[any_param, param2], executedActions=[action5=ACTIVE->COMPLETED]").build());

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

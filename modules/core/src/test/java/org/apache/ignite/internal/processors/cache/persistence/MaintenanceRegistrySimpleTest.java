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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.maintenance.MaintenanceProcessor;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Simple unit test to cover basic MaintenanceRegistry functionality like action validations,
 * maintenance tasks structure etc.
 */
public class MaintenanceRegistrySimpleTest {
    /** */
    private final IgniteLogger log = new GridTestLog4jLogger();

    /** */
    @Before
    public void beforeTest() throws Exception {
        cleanMaintenanceRegistryFile();
    }

    /** */
    @After
    public void afterTest() throws Exception {
        cleanMaintenanceRegistryFile();
    }

    /** */
    private void cleanMaintenanceRegistryFile() throws Exception {
        String dftlWorkDir = U.defaultWorkDirectory();

        for (File f : new File(dftlWorkDir).listFiles()) {
            if (f.getName().endsWith(".mntc"))
                f.delete();
        }
    }

    /** */
    private GridKernalContext initContext(boolean persistenceEnabled) throws IgniteCheckedException {
        String dfltWorkDir = U.defaultWorkDirectory();

        GridKernalContext kctx = new StandaloneGridKernalContext(log, null, null)
        {
            @Override protected IgniteConfiguration prepareIgniteConfiguration() {
                IgniteConfiguration cfg = super.prepareIgniteConfiguration();

                cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled)
                ));

                return cfg;
            }

            @Override public PdsFoldersResolver pdsFolderResolver() {
                return new PdsFoldersResolver() {
                    @Override public PdsFolderSettings resolveFolders() {
                        return new PdsFolderSettings(new File(dfltWorkDir), U.maskForFileName(""));
                    }
                };
            }
        };

        return kctx;
    }

    /**
     * {@link MaintenanceTask} could be replaced with new parameters after registration, old task is deleted.
     *
     * @throws IgniteCheckedException If initialization failed.
     */
    @Test
    public void testMaintenanceTaskReplacement() throws IgniteCheckedException {
        String name0 = "taskName0";
        String descr = "description";
        String oldParams = "oldParams";
        String newParams = "newParams";

        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        proc.start();

        assertFalse(proc.isMaintenanceMode());

        proc.registerMaintenanceTask(new MaintenanceTask(name0, descr, oldParams));
        proc.registerMaintenanceTask(new MaintenanceTask(name0, descr, newParams));

        proc.stop(false);

        proc.start();

        assertTrue(proc.isMaintenanceMode());
        MaintenanceTask task = proc.activeMaintenanceTask(name0);

        assertNotNull(task);
        assertEquals(newParams, task.parameters());
    }

    /**
     * Registered {@link MaintenanceTask} can be deleted before node entered Maintenance Mode (before node restart).
     *
     * @throws IgniteCheckedException If initialization failed.
     */
    @Test
    public void testDeleteMaintenanceTask() throws IgniteCheckedException {
        String name = "name0";

        MaintenanceTask task = new MaintenanceTask(name, "description", null);

        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        proc.start();

        proc.registerMaintenanceTask(task);

        assertFalse(proc.isMaintenanceMode());

        proc.unregisterMaintenanceTask(name);

        proc.stop(false);

        proc.start();

        assertNull(proc.activeMaintenanceTask(name));
        assertFalse(proc.isMaintenanceMode());
    }

    /**
     * Maintenance actions provided by maintenance callback should all have unique names.
     *
     * @throws IgniteCheckedException If initialization failed.
     */
    @Test
    public void testMaintenanceCallbackProvidesActionsWithUniqueNames() throws IgniteCheckedException {
        String actionName0 = "action0";
        String actionName1 = "action1";

        String name0 = "name0";
        String name1 = "name1";

        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        // attempt to register callback with actions with non-unique names throws exception
        GridTestUtils.assertThrows(
            log,
            () -> proc.registerWorkflowCallback(
                name0,
                new SimpleMaintenanceCallback(Arrays.asList(new SimpleAction(actionName0), new SimpleAction(actionName0)))
            ),
            IgniteException.class,
            "unique names: " + actionName0 + ", " + actionName0);

        // Attempt to register callback with actions with unique names finishes succesfully
        proc.registerWorkflowCallback(
            name1,
            new SimpleMaintenanceCallback(Arrays.asList(new SimpleAction(actionName0), new SimpleAction(actionName1)))
        );
    }

    /**
     * Smoke test for writing and restoring back maintenance tasks to/from file.
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMultipleMaintenanceTasks() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        String task0Name = "name0";
        String task1Name = "name1";

        String desc0 = "task0";
        String desc1 = "task1";

        String params0 = "task0_param";
        String params1 = "task1_param";

        proc.start();

        proc.registerMaintenanceTask(new MaintenanceTask(task0Name, desc0, params0));
        proc.registerMaintenanceTask(new MaintenanceTask(task1Name, desc1, params1));

        proc.stop(false);

        proc.start();

        MaintenanceTask task0 = proc.activeMaintenanceTask(task0Name);
        MaintenanceTask task1 = proc.activeMaintenanceTask(task1Name);

        assertNotNull(task0);
        assertNotNull(task1);

        assertEquals(desc0, task0.description());
        assertEquals(desc1, task1.description());

        assertEquals(params0, task0.parameters());
        assertEquals(params1, task1.parameters());
    }

    /**
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMaintenanceTasksWithoutParameters() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        String task0Name = "name0";
        String task1Name = "name1";

        String desc0 = "task0";
        String desc1 = "task1";

        String params0 = "task0_param";

        // call to initialize file for maintenance tasks
        proc.start();

        proc.registerMaintenanceTask(new MaintenanceTask(task0Name, desc0, params0));
        proc.registerMaintenanceTask(new MaintenanceTask(task1Name, desc1, null));

        proc.stop(false);

        // call to force Maintenance Processor to read that file and fill internal collection of maintenance tasks
        proc.start();

        MaintenanceTask task0 = proc.activeMaintenanceTask(task0Name);
        MaintenanceTask task1 = proc.activeMaintenanceTask(task1Name);

        assertNotNull(task0);
        assertNotNull(task1);

        assertEquals(params0, task0.parameters());
        assertNull(task1.parameters());
    }

    /**
     * Name of maintenance action should contain only alphanumeric and underscore symbols.
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMaintenanceActionNameSymbols() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        String name0 = "name0";
        String wrongName = "wrong*Name";

        GridTestUtils.assertThrows(log,
            () -> proc.registerWorkflowCallback(name0, new SimpleMaintenanceCallback(Arrays.asList(new SimpleAction(wrongName)))),
            IgniteException.class,
            "alphanumeric");

    }

    /** */
    private final class SimpleMaintenanceCallback implements MaintenanceWorkflowCallback {
        /** */
        private final List<MaintenanceAction<?>> actions = new ArrayList<>();

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
        @Override public @Nullable MaintenanceAction automaticAction() {
            return null;
        }
    }

    /** */
    private final class SimpleAction implements MaintenanceAction<Void> {
        /** */
        private final String name;

        /** */
        private SimpleAction(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public Void execute() {
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
}

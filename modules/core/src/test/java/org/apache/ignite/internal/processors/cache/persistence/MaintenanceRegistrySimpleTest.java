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
import org.apache.ignite.maintenance.MaintenanceRecord;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Simple unit test to cover basic MaintenanceRegistry functionality like action validations,
 * maintenance records structure etc.
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
     * Maintenance actions provided by maintenance callback should all have unique names.
     *
     * @throws IgniteCheckedException If initialization failed.
     */
    @Test
    public void testMaintenanceCallbackProvidesActionsWithUniqueNames() throws IgniteCheckedException {
        String actionName0 = "action0";
        String actionName1 = "action1";

        UUID id0 = UUID.randomUUID();
        UUID id1 = UUID.randomUUID();

        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        // attempt to register callback with actions with non-unique names throws exception
        GridTestUtils.assertThrows(log, () ->
                proc.registerWorkflowCallback(new SimpleMaintenanceCallback(id0,
                    Arrays.asList(new SimpleAction(actionName0), new SimpleAction(actionName0))
                )),
            IgniteException.class,
            "unique names: " + actionName0 + ", " + actionName0);

        // Attempt to register callback with actions with unique names finishes succesfully
        proc.registerWorkflowCallback(new SimpleMaintenanceCallback(id1,
            Arrays.asList(new SimpleAction(actionName0), new SimpleAction(actionName1))
            )
        );
    }

    /**
     * Smoke test for writing and restoring back maintenance records to/from file.
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMultipleMaintenanceRecords() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        UUID rec0Id = UUID.randomUUID();
        UUID rec1Id = UUID.randomUUID();

        String desc0 = "record0";
        String desc1 = "record1";

        String params0 = "rec0_param";
        String params1 = "rec1_param";

        proc.start();

        proc.registerMaintenanceRecord(new MaintenanceRecord(rec0Id, desc0, params0));
        proc.registerMaintenanceRecord(new MaintenanceRecord(rec1Id, desc1, params1));

        proc.start();

        MaintenanceRecord rec0 = proc.maintenanceRecord(rec0Id);
        MaintenanceRecord rec1 = proc.maintenanceRecord(rec1Id);

        assertNotNull(rec0);
        assertNotNull(rec1);

        assertEquals(desc0, rec0.description());
        assertEquals(desc1, rec1.description());

        assertEquals(params0, rec0.parameters());
        assertEquals(params1, rec1.parameters());
    }

    /**
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMaintenanceRecordsWithoutParameters() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        UUID rec0Id = UUID.randomUUID();
        UUID rec1Id = UUID.randomUUID();

        String desc0 = "record0";
        String desc1 = "record1";

        String params0 = "rec0_param";

        // call to initialize file for maintenance records
        proc.start();

        proc.registerMaintenanceRecord(new MaintenanceRecord(rec0Id, desc0, params0));
        proc.registerMaintenanceRecord(new MaintenanceRecord(rec1Id, desc1, null));

        // call to force Maintenance Processor to read that file and fill internal collection of maintenance records
        proc.start();

        MaintenanceRecord rec0 = proc.maintenanceRecord(rec0Id);
        MaintenanceRecord rec1 = proc.maintenanceRecord(rec1Id);

        assertNotNull(rec0);
        assertNotNull(rec1);

        assertEquals(params0, rec0.parameters());
        assertNull(rec1.parameters());
    }

    /**
     * Name of maintenance action should contain only alphanumeric and underscore symbols.
     *
     * @throws IgniteCheckedException If initialization of Maintenance Processor failed.
     */
    @Test
    public void testMaintenanceActionNameSymbols() throws IgniteCheckedException {
        MaintenanceProcessor proc = new MaintenanceProcessor(initContext(true));

        UUID id0 = UUID.randomUUID();
        String wrongName = "wrong*Name";

        GridTestUtils.assertThrows(log,
            () -> {
            proc.registerWorkflowCallback(new SimpleMaintenanceCallback(id0,
                Arrays.asList(new SimpleAction(wrongName))));
            },
            IgniteException.class,
            "alphanumeric");

    }

    /** */
    private final class SimpleMaintenanceCallback implements MaintenanceWorkflowCallback {
        /** */
        private final UUID id;

        /** */
        private final List<MaintenanceAction> actions = new ArrayList<>();

        SimpleMaintenanceCallback(UUID id, List<MaintenanceAction> actions) {
            this.id = id;
            this.actions.addAll(actions);
        }


        /** {@inheritDoc} */
        @Override public @NotNull UUID maintenanceId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean proceedWithMaintenance() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public @NotNull List<MaintenanceAction> allActions() {
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DFLT_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TRANSFER_RATE_BYTES;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_TRANSFER_RATE_DMS_KEY;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks command line property commands.
 */
public class GridCommandHandlerPropertiesTest extends GridCommandHandlerClusterByClassAbstractTest {

    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** */
    @After
    public void clear() {
    }

    /**
     * Check the command '--property help'.
     * Steps:
     */
    @Test
    public void testHelp() {
        Assume.assumeTrue(invoker.equals(CLI_CMD_HND));

        assertEquals(EXIT_CODE_OK, execute("--property", "help"));

        String out = testOut.toString();

        assertContains(log, out, "Print property command help:");
        assertContains(log, out, "control.(sh|bat) --property help");

        assertContains(log, out, "Print list of available properties:");
        assertContains(log, out, "control.(sh|bat) --property list");

        assertContains(log, out, "Get the property value:");
        assertContains(log, out, "control.(sh|bat) --property get --name <property_name>");

        assertContains(log, out, "Set the property value:");
        assertContains(log, out, "control.(sh|bat) --property set --name <property_name> --val <property_value>");
    }

    /**
     * Check the command '--property list'.
     * Steps:
     */
    @Test
    public void testList() {
        assertEquals(EXIT_CODE_OK, execute("--property", "list"));

        String out = testOut.toString();

        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties())
            assertContains(log, out, pd.getName());
    }

    /**
     * Check the command '--property get'.
     * Steps:
     */
    @Test
    public void testGet() {
        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties()) {
            assertEquals(EXIT_CODE_OK, execute("--property", "get", "--name", pd.getName()));
            String out = testOut.toString();

            assertContains(log, out, pd.getName() + " = " + pd.get());
        }
    }

    /**
     * Check the set command fro property 'sql.disabledFunctions'.
     * Steps:
     */
    @Test
    public void testPropertyDisabledSqlFunctions() {
        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "sql.disabledFunctions",
                "--val", "LENGTH, SESSION_ID"
            )
        );

        for (Ignite ign : G.allGrids()) {
            Set<String> disabledFuncs = ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                .distributedConfiguration().disabledFunctions();

            assertEquals(2, disabledFuncs.size());

            assertTrue(disabledFuncs.contains("LENGTH"));
            assertTrue(disabledFuncs.contains("SESSION_ID"));
        }
    }

    /**
     * Checks the set command for property 'checkpoint.deviation'.
     */
    @Test
    public void testPropertyCheckpointDeviation() throws IgniteCheckedException {
        String propName = "checkpoint.deviation";

        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            SimpleDistributedProperty<Integer> cpFreqDeviation = U.field(((IgniteEx)ign).context().cache().context().database(),
                "cpFreqDeviation");

            if (cpFreqDeviation.get() != null)
                ((IgniteEx)ign).context().distributedMetastorage().remove(propName);
        }

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", propName,
                "--val", "20"
            )
        );

        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            SimpleDistributedProperty<Integer> cpFreqDeviation = U.field(((IgniteEx)ign).context().cache().context().database(),
                "cpFreqDeviation");

            assertNotNull(cpFreqDeviation.get());

            assertEquals(20, cpFreqDeviation.get().intValue());
        }
    }

    /**
     * Check the set command fro property 'sql.defaultQueryTimeout'.
     * Steps:
     */
    @Test
    public void testPropertyDefaultQueryTimeout() {
        int dfltVal = ((IgniteH2Indexing)crd.context().query().getIndexing())
            .distributedConfiguration().defaultQueryTimeout();

        int newVal = dfltVal + 1000;

        assertEquals(EXIT_CODE_OK, execute("--property", "set", "--name", "sql.defaultQueryTimeout", "--val",
            Integer.toString(newVal)));

        for (Ignite ign : G.allGrids()) {
            assertEquals(
                "Invalid default query timeout on node: " + ign.name(),
                newVal,
                ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                    .distributedConfiguration().defaultQueryTimeout()
            );
        }

        assertEquals(
            EXIT_CODE_INVALID_ARGUMENTS,
            execute(
                "--property", "set",
                "--name", "sql.defaultQueryTimeout",
                "--val", "invalidVal"
            )
        );
    }

    /**
     * Check the set command for property 'history.rebalance.threshold'.
     */
    @Test
    public void testPropertyWalRebalanceThreshold() {
        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY,
                "--val", Integer.toString(DFLT_PDS_WAL_REBALANCE_THRESHOLD)
            )
        );

        assertDistributedPropertyEquals(HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY, DFLT_PDS_WAL_REBALANCE_THRESHOLD);

        int newVal = DFLT_PDS_WAL_REBALANCE_THRESHOLD * 2;

        assertEquals(
                EXIT_CODE_OK,
                execute(
                        "--property", "set",
                        "--name", HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY,
                        "--val", Integer.toString(newVal)
                )
        );

        assertDistributedPropertyEquals(HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY, newVal);
    }

    /**
     * Check the set command for property 'snapshotTransferRate'.
     */
    @Test
    public void testPropertySnapshotTransferRate() {
        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", SNAPSHOT_TRANSFER_RATE_DMS_KEY,
                "--val", Long.toString(DFLT_SNAPSHOT_TRANSFER_RATE_BYTES)
            )
        );

        assertDistributedPropertyEquals(SNAPSHOT_TRANSFER_RATE_DMS_KEY, DFLT_SNAPSHOT_TRANSFER_RATE_BYTES);

        long newVal = 1024;

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", SNAPSHOT_TRANSFER_RATE_DMS_KEY,
                "--val", Long.toString(newVal)
            )
        );

        assertDistributedPropertyEquals(SNAPSHOT_TRANSFER_RATE_DMS_KEY, newVal);
    }

    /**
     * Validates that distributed property has specified value across all nodes.
     *
     * @param propName Distributed property name.
     * @param expected Expected property value.
     * @param <T> Property type.
     */
    private <T extends Serializable> void assertDistributedPropertyEquals(String propName, T expected) {
        for (Ignite ign : G.allGrids()) {
            IgniteEx ignEx = (IgniteEx)ign;

            if (ign.configuration().isClientMode())
                continue;

            DistributedChangeableProperty<Serializable> prop =
                    ignEx.context().distributedConfiguration().property(propName);

            assertEquals(
                "Validation has failed on the cluster node [name=" + ign.configuration().getIgniteInstanceName(),
                prop.get(),
                expected);
        }
    }
}

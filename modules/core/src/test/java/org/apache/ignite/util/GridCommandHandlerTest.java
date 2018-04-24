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

package org.apache.ignite.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;

/**
 * Command line handler test.
 */
public class GridCommandHandlerTest extends GridCommonAbstractTest {
    /**
     * @return Folder in work directory.
     * @throws IgniteCheckedException If failed to resolve folder name.
     */
    protected File folder(String folder) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), folder, false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "bltTest";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();
        dsCfg.setWalMode(WALMode.LOG_ONLY);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--activate"));

        assertTrue(ignite.cluster().active());
    }

    /**
     * @param args Arguments.
     * @return Result of execution.
     */
    protected int execute(String... args) {
        return execute(new ArrayList<>(Arrays.asList(args)));
    }

    /**
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(ArrayList<String> args) {
        // Add force to avoid interactive confirmation
        args.add("--force");

        return new CommandHandler().execute(args);
    }

    /**
     * Test deactivation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testDeactivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--deactivate"));

        assertFalse(ignite.cluster().active());
    }

    /**
     * Test cluster active state works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testState() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--state"));

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--state"));
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineCollect() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * @param ignites Ignites.
     * @return Local node consistent ID.
     */
    private String consistentIds(Ignite... ignites) {
        String res = "";

        for (Ignite ignite : ignites) {
            String consistentId = ignite.cluster().localNode().consistentId().toString();

            if (!F.isEmpty(res))
                res += ", ";

            res += consistentId;
        }

        return res;
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineAdd() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineAddOnNotActiveCluster() throws Exception {
        PrintStream sysOut = System.out;

        try {
            Ignite ignite = startGrid(1);

            assertFalse(ignite.cluster().active());

            String consistentIDs = getTestIgniteInstanceName(1);

            ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
            System.setOut(new PrintStream(out));

            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

            assertTrue(out.toString().contains("Changing BaselineTopology on inactive cluster is not allowed."));

            consistentIDs =
                getTestIgniteInstanceName(1) + ", " +
                    getTestIgniteInstanceName(2) + "," +
                    getTestIgniteInstanceName(3);

            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

            assertTrue(out.toString().contains("Node not found for consistent ID: bltTest2"));
        }
        finally {
            System.setOut(sysOut);
        }
    }

    /**
     * Test baseline remove works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineRemove() throws Exception {
        Ignite ignite = startGrids(1);
        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        String offlineNodeConsId = consistentIds(other);

        stopGrid("nodeToStop");

        assertEquals(EXIT_CODE_OK, execute("--baseline"));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "remove", offlineNodeConsId));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline set works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineSet() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "set", consistentIds(ignite, other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "set", "invalidConsistentId"));
    }

    /**
     * Test baseline set by topology version works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineVersion() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(EXIT_CODE_OK, execute("--baseline", "version", String.valueOf(ignite.cluster().topologyVersion())));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheHelp() throws Exception {
        PrintStream sysOut = System.out;
        ByteArrayOutputStream out = null;

        try {
            Ignite ignite = startGrids(1);

            ignite.cluster().active(true);

            out = new ByteArrayOutputStream(4096);
            System.setOut(new PrintStream(out));

            assertEquals(EXIT_CODE_OK, execute("--cache", "help"));

            for (CacheCommand cmd : CacheCommand.values()) {
                if (cmd != CacheCommand.HELP)
                    assertTrue(cmd.text(), out.toString().contains(cmd.text()));
            }
        }
        finally {
            System.setOut(sysOut);

            if (out != null)
                System.out.println(out.toString());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIdleVerify() throws Exception {
        PrintStream sysOut = System.out;
        ByteArrayOutputStream out = null;

        try {
            Ignite ignite = startGrids(2);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1)
                .setName("cacheIV"));

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            out = new ByteArrayOutputStream(4096);
            System.setOut(new PrintStream(out));

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

            assertTrue(out.toString().contains("no conflicts have been found"));

            HashSet<Integer> clearKeys = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6));

            ((IgniteEx)ignite).context().cache().cache("cacheIV").clearLocallyAll(clearKeys, true, true, true);

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

            assertTrue(out.toString().contains("conflict partitions"));
        }
        finally {
            System.setOut(sysOut);

            if (out != null)
                System.out.println(out.toString());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIdleAnalyze() throws Exception {
        PrintStream sysOut = System.out;
        ByteArrayOutputStream out = null;

        try {
            Ignite ignite = startGrids(2);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1)
                .setName("cacheIA"));

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            out = new ByteArrayOutputStream(4096);
            System.setOut(new PrintStream(out));

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_analyze", String.valueOf(CU.cacheId("cacheIA")), "1"));

            assertTrue(out.toString().contains("no coflicts have been found in partition"));

            HashSet<Integer> clearKeys = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6));

            ((IgniteEx)ignite).context().cache().cache("cacheIA").clearLocallyAll(clearKeys, true, true, true);

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_analyze", String.valueOf(CU.cacheId("cacheIA")), "1"));

            assertTrue(out.toString().contains("conflict keys"));
        }
        finally {
            System.setOut(sysOut);

            if (out != null)
                System.out.println(out.toString());
        }
    }
}

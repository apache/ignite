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

import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** */
public class GridCommandHandlerWalTest extends GridCommandHandlerAbstractTest {
    /** */
    private int clusterState;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (clusterState == 1)
            cfg.setDataStorageConfiguration(null);
        else if (clusterState == 2) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setCdcEnabled(true)
                    .setPersistenceEnabled(false)));
        }
        else {
            cfg.getDataStorageConfiguration().setWalMode(getTestIgniteInstanceName(0).equals(igniteInstanceName)
                ? WALMode.BACKGROUND
                : WALMode.LOG_ONLY);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        injectTestSystemOut();

        super.beforeTest();
    }

    /** */
    @Test
    public void testWalStatePersistenceCluster() throws Exception {
        clusterState = 0; // PDS cluster.

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--wal", "state"));

        String out = testOut.toString();

        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(0) + ".*" + WALMode.BACKGROUND);
        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(1) + ".*" + WALMode.LOG_ONLY);
        outputContains(CU.UTILITY_CACHE_NAME + ".*true.*true.*true.*true.*false");

        srv.createCache("cache1");
        srv.createCache("cache2");
        srv.createCache("cache3");

        assertEquals(EXIT_CODE_OK, execute("--wal", "disable", "--groups", "cache2"));
        assertEquals(EXIT_CODE_OK, execute("--wal", "state"));

        outputContains(".*cache2.*true.*false.*true.*true.*false");

        assertEquals(EXIT_CODE_OK, execute("--wal", "enable", "--groups", "cache2"));
        assertEquals(EXIT_CODE_OK, execute("--wal", "state", "--groups", "cache1,cache2"));

        outputContains(".*cache1.*true.*true.*true.*true.*false");
        outputContains(".*cache2.*true.*true.*true.*true.*false");

        assertFalse(testOut.toString().contains("cache3"));
    }

    /** */
    @Test
    public void testWalStateInMemoryCluster() throws Exception {
        clusterState = 1; // In-memory.

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.createCache("cache1");

        assertEquals(EXIT_CODE_OK, execute("--wal", "state"));

        String out = testOut.toString();

        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(0) + ".*null");
        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(1) + ".*null");
        outputContains(CU.UTILITY_CACHE_NAME + ".*false.*false.*true.*true.*false");
        outputContains("cache1.*false.*false.*true.*true.*false");

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--wal", "--disable", "--groups", CU.UTILITY_CACHE_NAME));
    }

    /** */
    @Test
    public void testWalStateInMemoryCdcCluster() throws Exception {
        clusterState = 2; // In-memory CDC.

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--wal", "state"));

        String out = testOut.toString();

        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(0) + ".*" + WALMode.LOG_ONLY);
        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(1) + ".*" + WALMode.LOG_ONLY);
        outputContains(CU.UTILITY_CACHE_NAME + ".*false.*true.*true.*true.*false");

        srv.createCache("cache1");
        srv.createCache("cache2");
        srv.createCache("cache3");

        assertEquals(EXIT_CODE_OK, execute("--wal", "disable", "--groups", "cache2"));
        assertEquals(EXIT_CODE_OK, execute("--wal", "state"));

        outputContains(".*cache2.*false.*true.*true.*true.*true");

        assertEquals(EXIT_CODE_OK, execute("--wal", "enable", "--groups", "cache2"));
        assertEquals(EXIT_CODE_OK, execute("--wal", "state", "--groups", "cache1,cache2"));

        outputContains(".*cache1.*false.*true.*true.*true.*true");
        outputContains(".*cache2.*false.*true.*true.*true.*true");

        assertFalse(testOut.toString().contains("cache3"));
    }

    /** */
    private void outputContains(String regexp) {
        assertTrue(Pattern.compile(regexp).matcher(testOut.toString()).find());
    }
}

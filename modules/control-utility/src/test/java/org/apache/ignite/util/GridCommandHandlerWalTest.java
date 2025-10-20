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

/** */
public class GridCommandHandlerWalTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setWalMode(getTestIgniteInstanceName(0).equals(igniteInstanceName)
                ? WALMode.BACKGROUND
                : WALMode.LOG_ONLY);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!getTestIgniteInstanceName(3).equals(igniteInstanceName))
            cfg.setDataStorageConfiguration(dsCfg);

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
    public void testWalStateInPersistenceCluster() throws Exception {
        IgniteEx srv = startGrids(3); // Third node without persistence.
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(0, execute("--wal", "state"));

        String out = testOut.toString();

        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(0) + ".*" + WALMode.BACKGROUND);
        outputContains("Node \\[consistentId=" + getTestIgniteInstanceName(1) + ".*" + WALMode.LOG_ONLY);
        outputContains(CU.UTILITY_CACHE_NAME + ".*true.*true.*true");

        srv.createCache("cache1");
        srv.createCache("cache2");
        srv.createCache("cache3");

        srv.cluster().disableWal("cache2");

        assertEquals(0, execute("--wal", "state"));

        outputContains(".*cache2.*false.*true.*true");

        srv.cluster().enableWal("cache2");

        assertEquals(0, execute("--wal", "state", "--groups", "cache1,cache2"));

        outputContains(".*cache1.*true.*true.*true");
        outputContains(".*cache2.*true.*true.*true");

        assertFalse(testOut.toString().contains("cache3"));
    }

    /** */
    private void outputContains(String regexp) {
        assertTrue(Pattern.compile(regexp).matcher(testOut.toString()).find());
    }
}

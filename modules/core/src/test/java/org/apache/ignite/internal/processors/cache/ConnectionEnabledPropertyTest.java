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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.toMetaStorageKey;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class ConnectionEnabledPropertyTest extends GridCommonAbstractTest {
    /** */
    private static final String SRV_CONN_ENABLED_PROP = "newServerNodeConnectionsEnabled";

    /** */
    private static final String CLI_CONN_ENABLED_PROP = "newClientNodeConnectionsEnabled";

    /** */
    @Parameterized.Parameter
    public boolean persistence;

    /** */
    @Parameterized.Parameters(name = "persistence={0}")
    public static Iterable<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (persistence) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration());
            cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testConnectionEnabledProperty() throws Exception {
        RunnableX srvCanJoin = () -> {
            try (Ignite srv1 = startGrid(1)) {
                assertNotNull(srv1.cacheNames());
            }
        };

        RunnableX cliCanJoin = () -> {
            try (Ignite cli = startClientGrid(2)) {
                assertNotNull(cli.cacheNames());
            }
        };

        // Two iteration to check successfull restart when newServerNodeConnectionsEnabled=false.
        for (int i = 0; i < 2; i++) {
            try (IgniteEx srv = startGrid(0)) {
                srv.cluster().state(ClusterState.ACTIVE);

                if (persistence && i == 1) {
                    assertFalse(srv.context().distributedMetastorage().read(toMetaStorageKey(SRV_CONN_ENABLED_PROP)));
                    assertFalse(srv.context().distributedMetastorage().read(toMetaStorageKey(CLI_CONN_ENABLED_PROP)));

                    srv.context().distributedMetastorage().write(toMetaStorageKey(SRV_CONN_ENABLED_PROP), true);
                    srv.context().distributedMetastorage().write(toMetaStorageKey(CLI_CONN_ENABLED_PROP), true);
                }

                assertTrue(srv.context().distributedMetastorage().read(toMetaStorageKey(SRV_CONN_ENABLED_PROP)));
                assertTrue(srv.context().distributedMetastorage().read(toMetaStorageKey(CLI_CONN_ENABLED_PROP)));

                srv.context().distributedMetastorage().write(toMetaStorageKey(SRV_CONN_ENABLED_PROP), true);
                srv.context().distributedMetastorage().write(toMetaStorageKey(CLI_CONN_ENABLED_PROP), true);

                srvCanJoin.run();
                cliCanJoin.run();

                srv.context().distributedMetastorage().write(toMetaStorageKey(SRV_CONN_ENABLED_PROP), false);

                assertThrowsWithCause(srvCanJoin, IgniteCheckedException.class);
                cliCanJoin.run();

                srv.context().distributedMetastorage().write(toMetaStorageKey(CLI_CONN_ENABLED_PROP), false);

                assertThrowsWithCause(srvCanJoin, IgniteCheckedException.class);
                assertThrowsWithCause(cliCanJoin, IgniteCheckedException.class);

                srv.context().distributedMetastorage().write(toMetaStorageKey(SRV_CONN_ENABLED_PROP), true);

                srvCanJoin.run();
                assertThrowsWithCause(cliCanJoin, IgniteCheckedException.class);

                srv.context().distributedMetastorage().write(toMetaStorageKey(CLI_CONN_ENABLED_PROP), true);

                srvCanJoin.run();
                cliCanJoin.run();

                if (persistence) {
                    srv.context().distributedMetastorage().write(toMetaStorageKey(SRV_CONN_ENABLED_PROP), false);
                    srv.context().distributedMetastorage().write(toMetaStorageKey(CLI_CONN_ENABLED_PROP), false);

                    assertTrue(waitForCondition(() -> {
                        try {
                            return !srv.context().distributedMetastorage().<Boolean>read(toMetaStorageKey(SRV_CONN_ENABLED_PROP)) &&
                                !srv.context().distributedMetastorage().<Boolean>read(toMetaStorageKey(CLI_CONN_ENABLED_PROP));
                        }
                        catch (IgniteCheckedException e) {
                            throw new RuntimeException(e);
                        }
                    }, 30_000));
                }
            }
        }
    }
}

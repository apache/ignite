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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISTRIBUTED_METASTORAGE_KEYS_TO_SKIP;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests the distributed metastorage keys skip.
 *
 * @see IgniteSystemProperties#IGNITE_DISTRIBUTED_METASTORAGE_KEYS_TO_SKIP
 */
public class DistributedMetaStorageSkipKeysTest extends GridCommonAbstractTest {
    /** Test key 1. (For a value with unknown class after recovery. */
    private static final String KEY_1 = "test-unknown-class-key-1";

    /** Test value 1 classname. */
    private static final String VALUE_1_CLASSNAME = "org.apache.ignite.tests.p2p.TestUserResource";

    /** Test key 2. */
    private static final String KEY_2 = "test-key-2";

    /** Test value 2. */
    private static final String VALUE_2 = "test-value-2";

    /** True if start nodes in the local JVM. */
    private static boolean startLocalNode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
            )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return super.isRemoteJvm(igniteInstanceName) && !startLocalNode;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmClasspath() {
        return Collections.singletonList(GridTestProperties.getProperty("p2p.uri.classpath"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();

        startLocalNode = false;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSkipKey() throws Exception {
        assertFalse(U.inClassPath(VALUE_1_CLASSNAME));

        // 1. Start remote JVM ignite instance and write to the metastorage a value with class unknown to a local JVM.
        IgniteEx ignite = startGrid(0);

        IgniteProcessProxy rmtIgnite = (IgniteProcessProxy)startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.compute(ignite.cluster().forRemotes()).broadcast(new WriteMetastorageJob());

        // Stop local instance to prevent transfer the value with class unknown to a local JVM.
        stopGrid(0, true);

        // 2. Remote node will write entries to metastorage on node left event and will be stopped.
        GridTestUtils.waitForCondition(() -> !rmtIgnite.getProcess().getProcess().isAlive(), getTestTimeout());

        assertTrue(G.allGrids().isEmpty());

        System.setProperty(IGNITE_DISTRIBUTED_METASTORAGE_KEYS_TO_SKIP, KEY_1);

        try {
            // 3. Recovery metastorage from remote node data and check on local JVM.
            startLocalNode = true;

            ignite = startGrid(0);

            startGrid(1);

            ignite.cluster().state(ClusterState.ACTIVE);

            DistributedMetaStorage metastorage = ignite.context().distributedMetastorage();

            metastorage.iterate(KEY_1, (key, val) -> fail());

            assertEquals(VALUE_2, metastorage.read(KEY_2));
        }
        finally {
            System.clearProperty(IGNITE_DISTRIBUTED_METASTORAGE_KEYS_TO_SKIP);
        }
    }

    /** Job for a remote JVM Ignite instance to add event listener. */
    private static class WriteMetastorageJob implements IgniteRunnable {
        /** Auto injected ignite instance. */
        @IgniteInstanceResource
        IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            assertTrue(U.inClassPath(VALUE_1_CLASSNAME));

            ignite.events().localListen(event -> {
                try {
                    Serializable val1 = U.newInstance(VALUE_1_CLASSNAME);

                    ignite.context().distributedMetastorage().write(KEY_1, val1);

                    ignite.context().distributedMetastorage().write(KEY_2, VALUE_2);

                    CompletableFuture.runAsync(() -> G.stop(ignite.name(), false));
                }
                catch (Exception e) {
                    ignite.log().error("Unexpected error.", e);
                }

                return false;
            }, EVT_NODE_LEFT);
        }
    }
}

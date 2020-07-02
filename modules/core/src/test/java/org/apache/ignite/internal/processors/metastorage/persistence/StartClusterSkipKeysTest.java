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
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests metastorage restore from previous cluster with different class path.
 */
public class StartClusterSkipKeysTest extends GridCommonAbstractTest {
    /** Test key 1. (For a value with unknown class. */
    private static final String KEY_1 = "test-unknown-class-key-1";

    /** Test value 1 classname. */
    private static final String VALUE_1_CLASSNAME = "org.apache.ignite.tests.p2p.TestUserResource";

    /** Test key 2. */
    private static final String KEY_2 = "test-key-2";

    /** Test value 2. */
    private static final String VALUE_2 = "test-value-2";

    /** Job start latch. */
    private static final String JOB_LATCH = "job-start-latch";

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

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartWithUnknownKey() throws Exception {
        assertFalse(classFound(getClass().getClassLoader(), VALUE_1_CLASSNAME));

        // 1. Start remote JVM ignite instance and write to the metastorage a value with class unknown to a local JVM.
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteProcessProxy rmtIgnite = (IgniteProcessProxy)startGrid(1);

        IgniteCountDownLatch latch = ignite.countDownLatch(JOB_LATCH, 1, false, true);

        ignite.compute(ignite.cluster().forRemotes()).broadcastAsync(new WriteMetastorageJob());

        // Stop local instance to prevent transfer a value with class unknown to a local JVM.
        latch.await();

        stopGrid(0, true);

        // Make sure that job was finished.
        U.sleep(5000);

        // 2. Stop remote node.
        rmtIgnite.getProcess().kill();

        // 3. Recovery metastorage from remote node data and check metastorage.
        startLocalNode = true;

        ignite = startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        startGrid(0);

        waitForTopology(2);

        assertEquals(VALUE_2, ignite.context().distributedMetastorage().read(KEY_2));

//        System.out.println("MY CHECK2="+ ignite.context().distributedMetastorage().read(TEST_KEY_UNKNOWN_CLASS));
    }

    /**
     * @param clsLdr classloader.
     * @param name classname.
     * @return true if class loaded by classloader, false if class not found.
     */
    private boolean classFound(ClassLoader clsLdr, String name) {
        try {
            clsLdr.loadClass(name);

            return true;
        }
        catch (ClassNotFoundException e) {
            return false;
        }
    }

    /** Job to write to metastorage on a remote grid. */
    private static class WriteMetastorageJob implements IgniteRunnable, ComputeJobMasterLeaveAware {
        /** Auto injected ignite instance. */
        @IgniteInstanceResource
        IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCountDownLatch latch = ignite.countDownLatch(JOB_LATCH, 0, false, false);

            latch.countDown();

            try {
                // Waiting for master node left.
                while (!Thread.interrupted())
                    U.sleep(100);
            }
            catch (Exception ignored) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) throws IgniteException {
            try {
                Serializable val1 = U.newInstance(VALUE_1_CLASSNAME);

                ignite.context().distributedMetastorage().write(KEY_1, val1);

                ignite.context().distributedMetastorage().write(KEY_2, VALUE_2);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.IgfsEventsAbstractSelfTest;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.G;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Test suite for IGFS event tests.
 */
@SuppressWarnings("PublicInnerClass")
@RunWith(AllTests.class)
public class IgfsEventsTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite FS Events Test Suite");

        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(ShmemPrimary.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(ShmemDualSync.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(ShmemDualAsync.class.getName())));

        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackPrimary.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackDualSync.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackDualAsync.class.getName())));

        return suite;
    }

    /**
     * @return Test suite with only tests that are supported on all platforms.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suiteNoarchOnly() throws Exception {
        ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite IGFS Events Test Suite Noarch Only");

        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackPrimary.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackDualSync.class.getName())));
        suite.addTest(new JUnit4TestAdapter(ldr.loadClass(LoopbackDualAsync.class.getName())));

        return suite;
    }

    /**
     * Shared memory IPC in PRIVATE mode.
     */
    public static class ShmemPrimary extends IgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(IgfsMode.PRIMARY);

            IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

            endpointCfg.setType(IgfsIpcEndpointType.SHMEM);
            endpointCfg.setPort(IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1);

            igfsCfg.setIpcEndpointConfiguration(endpointCfg);

            return igfsCfg;
        }
    }

    /**
     * Loopback socket IPS in PRIVATE mode.
     */
    public static class LoopbackPrimary extends IgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(IgfsMode.PRIMARY);

            IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

            endpointCfg.setType(IgfsIpcEndpointType.TCP);
            endpointCfg.setPort(IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1);

            igfsCfg.setIpcEndpointConfiguration(endpointCfg);

            return igfsCfg;
        }
    }

    /**
     * Base class for all IGFS tests with primary and secondary file system.
     */
    public abstract static class PrimarySecondaryTest extends IgfsEventsAbstractSelfTest {
        /** Secondary file system. */
        private static IgniteFileSystem igfsSec;

        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setSecondaryFileSystem(new IgniteHadoopIgfsSecondaryFileSystem(
                "igfs://igfs-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-secondary.xml"));

            return igfsCfg;
        }

        /**
         * @return IGFS configuration for secondary file system.
         */
        protected FileSystemConfiguration getSecondaryIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setName("igfs-secondary");
            igfsCfg.setDefaultMode(PRIMARY);

            IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

            endpointCfg.setType(IgfsIpcEndpointType.TCP);
            endpointCfg.setPort(11500);

            igfsCfg.setIpcEndpointConfiguration(endpointCfg);

            return igfsCfg;
        }

        /** {@inheritDoc} */
        @Override protected void beforeTestsStarted() throws Exception {
            igfsSec = startSecondary();

            super.beforeTestsStarted();
        }

        /** {@inheritDoc} */
        @Override protected void afterTestsStopped() throws Exception {
            G.stopAll(true);
        }

        /** {@inheritDoc} */
        @Override protected void afterTest() throws Exception {
            super.afterTest();

            // Clean up secondary file system.
            igfsSec.clear();
        }

        /**
         * Start a grid with the secondary file system.
         *
         * @return Secondary file system handle.
         * @throws Exception If failed.
         */
        @Nullable private IgniteFileSystem startSecondary() throws Exception {
            IgniteConfiguration cfg = getConfiguration("grid-secondary", getSecondaryIgfsConfiguration());

            cfg.setLocalHost("127.0.0.1");
            cfg.setPeerClassLoadingEnabled(false);

            Ignite secG = G.start(cfg);

            return secG.fileSystem("igfs-secondary");
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualSync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(DUAL_SYNC);

            return igfsCfg;
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualAsync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(DUAL_ASYNC);

            return igfsCfg;
        }
    }

    /**
     * Loopback socket IPC with secondary file system.
     */
    public abstract static class LoopbackPrimarySecondaryTest extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(IgfsMode.PRIMARY);

            igfsCfg.setSecondaryFileSystem(new IgniteHadoopIgfsSecondaryFileSystem(
                "igfs://igfs-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

            return igfsCfg;
        }

        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getSecondaryIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getSecondaryIgfsConfiguration();

            igfsCfg.setName("igfs-secondary");
            igfsCfg.setDefaultMode(PRIMARY);

            IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

            endpointCfg.setType(IgfsIpcEndpointType.TCP);
            endpointCfg.setPort(11500);

            igfsCfg.setIpcEndpointConfiguration(endpointCfg);

            return igfsCfg;
        }
    }

    /**
     * Loopback IPC in DUAL_SYNC mode.
     */
    public static class LoopbackDualSync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(DUAL_SYNC);

            return igfsCfg;
        }
    }

    /**
     * Loopback socket IPC in DUAL_ASYNC mode.
     */
    public static class LoopbackDualAsync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

            igfsCfg.setDefaultMode(DUAL_ASYNC);

            return igfsCfg;
        }
    }
}

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

package org.apache.ignite.igfs;

import junit.framework.TestSuite;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.G;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Test suite for IGFS event tests.
 */
@SuppressWarnings("PublicInnerClass")
public class IgfsEventsTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite FS Events Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(ShmemPrivate.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(ShmemDualSync.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(ShmemDualAsync.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(LoopbackPrivate.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualSync.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualAsync.class.getName())));

        return suite;
    }

    /**
     * @return Test suite with only tests that are supported on all platforms.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suiteNoarchOnly() throws Exception {
        ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite IGFS Events Test Suite Noarch Only");

        suite.addTest(new TestSuite(ldr.loadClass(LoopbackPrivate.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualSync.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualAsync.class.getName())));

        return suite;
    }

    /**
     * Shared memory IPC in PRIVATE mode.
     */
    public static class ShmemPrivate extends IgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

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
    public static class LoopbackPrivate extends IgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected FileSystemConfiguration getIgfsConfiguration() throws IgniteCheckedException {
            FileSystemConfiguration igfsCfg = super.getIgfsConfiguration();

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
                "igfs://igfs-secondary:grid-secondary@127.0.0.1:11500/",
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
            super.afterTestsStopped();

            G.stopAll(true);
        }

        /** {@inheritDoc} */
        @Override protected void afterTest() throws Exception {
            super.afterTest();

            // Clean up secondary file system.
            igfsSec.format();
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

            igfsCfg.setSecondaryFileSystem(new IgniteHadoopIgfsSecondaryFileSystem(
                "igfs://igfs-secondary:grid-secondary@127.0.0.1:11500/",
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
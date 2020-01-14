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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Test suite for IGFS event tests.
 */
@RunWith(DynamicSuite.class)
public class IgfsEventsTestSuite {
    /**
     * @return Test suite.
     * @throws ClassNotFoundException If the class was not found by class loader.
     */
    public static List<Class<?>> suite() throws ClassNotFoundException {
        ClassLoader ldr = IgfsEventsTestSuite.class.getClassLoader();

        List<Class<?>> suite = new ArrayList<>();

        suite.add(ldr.loadClass(ShmemPrimary.class.getName()));
        suite.add(ldr.loadClass(ShmemDualSync.class.getName()));
        suite.add(ldr.loadClass(ShmemDualAsync.class.getName()));

        suite.add(ldr.loadClass(LoopbackPrimary.class.getName()));
        suite.add(ldr.loadClass(LoopbackDualSync.class.getName()));
        suite.add(ldr.loadClass(LoopbackDualAsync.class.getName()));

        return suite;
    }

    /**
     * @return Test suite with only tests that are supported on all platforms.
     * @throws ClassNotFoundException If the class was not found by class loader.
     */
    private static List<Class<?>> suiteNoarchOnly() throws ClassNotFoundException {
        ClassLoader ldr = IgfsEventsTestSuite.class.getClassLoader();

        List<Class<?>> suite = new ArrayList<>();

        suite.add(ldr.loadClass(LoopbackPrimary.class.getName()));
        suite.add(ldr.loadClass(LoopbackDualSync.class.getName()));
        suite.add(ldr.loadClass(LoopbackDualAsync.class.getName()));

        return suite;
    }

    /** */
    @RunWith(IgfsEventsTestSuite.IgfsEventsNoarchOnlyTestSuite.class)
    public static class IgfsEventsNoarchOnlyTest {
    }

    /** */
    public static class IgfsEventsNoarchOnlyTestSuite extends Suite {
        /** */
        public IgfsEventsNoarchOnlyTestSuite(Class<?> cls) throws ClassNotFoundException, InitializationError {
            super(cls, suiteNoarchOnly().toArray(new Class<?>[] {null}));
        }
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

            //noinspection deprecation
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
        @Override protected void afterTestsStopped() {
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

            //noinspection deprecation
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

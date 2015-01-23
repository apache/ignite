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

package org.apache.ignite.fs;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.ipc.shmem.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 * Test suite for GGFS event tests.
 */
@SuppressWarnings("PublicInnerClass")
public class GridGgfsEventsTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        TestSuite suite = new TestSuite("Gridgain GGFS Events Test Suite");

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
        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        TestSuite suite = new TestSuite("Gridgain GGFS Events Test Suite Noarch Only");

        suite.addTest(new TestSuite(ldr.loadClass(LoopbackPrivate.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualSync.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(LoopbackDualAsync.class.getName())));

        return suite;
    }

    /**
     * Shared memory IPC in PRIVATE mode.
     */
    public static class ShmemPrivate extends GridGgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
                put("type", "shmem");
                put("port", String.valueOf(GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1));
            }});

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPS in PRIVATE mode.
     */
    public static class LoopbackPrivate extends GridGgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
                put("type", "tcp");
                put("port", String.valueOf(GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1));
            }});

            return ggfsCfg;
        }
    }

    /**
     * Base class for all GGFS tests with primary and secondary file system.
     */
    public abstract static class PrimarySecondaryTest extends GridGgfsEventsAbstractSelfTest {
        /** Secondary file system. */
        private static IgniteFs ggfsSec;

        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
                "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-secondary.xml"));

            return ggfsCfg;
        }

        /**
         * @return GGFS configuration for secondary file system.
         */
        protected IgniteFsConfiguration getSecondaryGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setName("ggfs-secondary");
            ggfsCfg.setDefaultMode(PRIMARY);
            ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>(){{
                put("type", "tcp");
                put("port", "11500");
            }});

            return ggfsCfg;
        }

        /** {@inheritDoc} */
        @Override protected void beforeTestsStarted() throws Exception {
            ggfsSec = startSecondary();

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
            ggfsSec.format();
        }

        /**
         * Start a grid with the secondary file system.
         *
         * @return Secondary file system handle.
         * @throws Exception If failed.
         */
        @Nullable private IgniteFs startSecondary() throws Exception {
            IgniteConfiguration cfg = getConfiguration("grid-secondary", getSecondaryGgfsConfiguration());

            cfg.setLocalHost("127.0.0.1");
            cfg.setPeerClassLoadingEnabled(false);

            Ignite secG = G.start(cfg);

            return secG.fileSystem("ggfs-secondary");
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualSync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_SYNC);

            return ggfsCfg;
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualAsync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_ASYNC);

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPC with secondary file system.
     */
    public abstract static class LoopbackPrimarySecondaryTest extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
                "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

            return ggfsCfg;
        }

        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getSecondaryGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getSecondaryGgfsConfiguration();

            ggfsCfg.setName("ggfs-secondary");
            ggfsCfg.setDefaultMode(PRIMARY);
            ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
                put("type", "tcp");
                put("port", "11500");
            }});

            return ggfsCfg;
        }
    }

    /**
     * Loopback IPC in DUAL_SYNC mode.
     */
    public static class LoopbackDualSync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_SYNC);

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPC in DUAL_ASYNC mode.
     */
    public static class LoopbackDualAsync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected IgniteFsConfiguration getGgfsConfiguration() throws IgniteCheckedException {
            IgniteFsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_ASYNC);

            return ggfsCfg;
        }
    }
}

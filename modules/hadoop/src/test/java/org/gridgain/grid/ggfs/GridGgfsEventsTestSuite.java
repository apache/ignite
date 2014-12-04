/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import junit.framework.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.ggfs.GridGgfsMode.*;

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
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'shmem', port:" +
                (GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1) + "}"));

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPS in PRIVATE mode.
     */
    public static class LoopbackPrivate extends GridGgfsEventsAbstractSelfTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:" +
                (GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + 1) + "}"));

            return ggfsCfg;
        }
    }

    /**
     * Base class for all GGFS tests with primary and secondary file system.
     */
    public abstract static class PrimarySecondaryTest extends GridGgfsEventsAbstractSelfTest {
        /** Secondary file system. */
        private static GridGgfs ggfsSec;

        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
                "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-secondary.xml"));

            return ggfsCfg;
        }

        /**
         * @return GGFS configuration for secondary file system.
         */
        protected GridGgfsConfiguration getSecondaryGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setName("ggfs-secondary");
            ggfsCfg.setDefaultMode(PRIMARY);
            ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:11500}"));

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
        @Nullable private GridGgfs startSecondary() throws Exception {
            IgniteConfiguration cfg = getConfiguration("grid-secondary", getSecondaryGgfsConfiguration());

            cfg.setLocalHost("127.0.0.1");
            cfg.setPeerClassLoadingEnabled(false);

            Ignite secG = G.start(cfg);

            return secG.ggfs("ggfs-secondary");
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualSync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_SYNC);

            return ggfsCfg;
        }
    }

    /**
     * Shared memory IPC in DUAL_SYNC mode.
     */
    public static class ShmemDualAsync extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_ASYNC);

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPC with secondary file system.
     */
    public abstract static class LoopbackPrimarySecondaryTest extends PrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
                "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/",
                "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

            return ggfsCfg;
        }

        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getSecondaryGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getSecondaryGgfsConfiguration();

            ggfsCfg.setName("ggfs-secondary");
            ggfsCfg.setDefaultMode(PRIMARY);
            ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:11500}"));

            return ggfsCfg;
        }
    }

    /**
     * Loopback IPC in DUAL_SYNC mode.
     */
    public static class LoopbackDualSync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_SYNC);

            return ggfsCfg;
        }
    }

    /**
     * Loopback socket IPC in DUAL_ASYNC mode.
     */
    public static class LoopbackDualAsync extends LoopbackPrimarySecondaryTest {
        /** {@inheritDoc} */
        @Override protected GridGgfsConfiguration getGgfsConfiguration() throws GridException {
            GridGgfsConfiguration ggfsCfg = super.getGgfsConfiguration();

            ggfsCfg.setDefaultMode(DUAL_ASYNC);

            return ggfsCfg;
        }
    }
}

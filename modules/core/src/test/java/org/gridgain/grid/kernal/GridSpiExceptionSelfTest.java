/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.spi.eventstorage.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Tests exceptions that are thrown by event storage and deployment spi.
 */
@GridCommonTest(group = "Kernal Self")
public class GridSpiExceptionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_MSG = "Test exception message";

    /** */
    public GridSpiExceptionSelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setEventStorageSpi(new GridTestRuntimeExceptionSpi());
        cfg.setDeploymentSpi(new GridTestCheckedExceptionSpi());

        // Disable cache since it can deploy some classes during start process.
        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSpiFail() throws Exception {
        Ignite ignite = startGrid();

        try {
            try {
                ignite.events().localQuery(F.<GridEvent>alwaysTrue());

                assert false : "Exception should be thrown";
            }
            catch (GridRuntimeException e) {
                assert e.getMessage().startsWith(TEST_MSG) : "Wrong exception message." + e.getMessage();
            }

            try {
                ignite.compute().localDeployTask(GridTestTask.class, GridTestTask.class.getClassLoader());

                assert false : "Exception should be thrown";
            }
            catch (GridException e) {
                assert e.getCause() instanceof GridTestSpiException : "Wrong cause exception type. " + e;

                assert e.getCause().getMessage().startsWith(TEST_MSG) : "Wrong exception message." + e.getMessage();
            }
        }
        finally {
            stopGrid();
        }
    }

    /**
     * Test event storage spi that throws an exception on try to query local events.
     */
    @GridSpiMultipleInstancesSupport(true)
    private static class GridTestRuntimeExceptionSpi extends GridSpiAdapter implements GridEventStorageSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws GridSpiException {
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public <T extends GridEvent> Collection<T> localEvents(IgnitePredicate<T> p) {
            throw new GridRuntimeException(TEST_MSG);
        }

        /** {@inheritDoc} */
        @Override public void record(GridEvent evt) throws GridSpiException {
            // No-op.
        }
    }

    /**
     * Test deployment spi that throws an exception on try to register any class.
     */
    @GridSpiMultipleInstancesSupport(true)
    private static class GridTestCheckedExceptionSpi extends GridSpiAdapter implements GridDeploymentSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridDeploymentResource findResource(String rsrcName) {
            // No-op.
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean register(ClassLoader ldr, Class<?> rsrc) throws GridSpiException {
            throw new GridTestSpiException(TEST_MSG);
        }

        /** {@inheritDoc} */
        @Override public boolean unregister(String rsrcName) {
            // No-op.
            return false;
        }

        /** {@inheritDoc} */
        @Override public void setListener(GridDeploymentListener lsnr) {
            // No-op.
        }
    }

    /**
     * Test spi exception.
     */
    private static class GridTestSpiException extends GridSpiException {
        /**
         * @param msg Error message.
         */
        GridTestSpiException(String msg) {
            super(msg);
        }

        /**
         * @param msg Error message.
         * @param cause Error cause.
         */
        GridTestSpiException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}

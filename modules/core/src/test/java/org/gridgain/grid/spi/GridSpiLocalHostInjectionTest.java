/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

/**
 * This class tests injection of {@code localHost} property to various SPIs.
 */
public class GridSpiLocalHostInjectionTest extends GridCommonAbstractTest {
    /** Value to be set globally in config. */
    public static final String CONFIG_LOCAL_ADDR_VALUE = "127.0.0.3";

    /** Value to be set locally to SPI.before injection */
    public static final String SPI_LOCAL_ADDR_VALUE = "127.0.0.2";

    /**
     * @throws GridException If test fails.
     */
    public void testTcpDiscoverySpiBothSet() throws GridException {
        processTcpDiscoverySpiTestInjection(true, true, SPI_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpDiscoverySpiOnlySet() throws GridException {
        processTcpDiscoverySpiTestInjection(false, true, SPI_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpDiscoverySpiConfigOnlySet() throws GridException {
        processTcpDiscoverySpiTestInjection(true, false, CONFIG_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpDiscoverySpiBothNotSet() throws GridException {
        processTcpDiscoverySpiTestInjection(false, false, null);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpCommunicationSpiBothSet() throws GridException {
        processTcpCommunicationSpiTestInjection(true, true, SPI_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpCommunicationSpiOnlySet() throws GridException {
        processTcpCommunicationSpiTestInjection(false, true, SPI_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpCommunicationSpiConfigOnlySet() throws GridException {
        processTcpCommunicationSpiTestInjection(true, false, CONFIG_LOCAL_ADDR_VALUE);
    }

    /**
     * @throws GridException If test fails.
     */
    public void testTcpCommunicationSpiBothNotSet() throws GridException {
        processTcpCommunicationSpiTestInjection(false, false, null);
    }

    /**
     * Performs test of {@code localHost} resource injection for {@link GridTcpDiscoverySpi}.
     *
     * @param cfgVal {@code true} if {@code localHost} should be set in configuration adapter.
     * @param spiVal {@code true} if {@code localHost} should be set in SPI
     * @param exp Expected value of {@code localHost} property in SPI after injection.
     * @throws GridException If test fails.
     */
    private void processTcpDiscoverySpiTestInjection(boolean cfgVal, boolean spiVal, @Nullable String exp)
        throws GridException {
        GridResourceProcessor proc = getResourceProcessor(cfgVal);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        if (spiVal)
            spi.setLocalAddress(SPI_LOCAL_ADDR_VALUE);

        proc.inject(spi);

        assertEquals("Invalid localAddr value after injection: ", exp, spi.getLocalAddress());
    }

    /**
     * Performs test of {@code localHost} resource injection for {@link GridTcpCommunicationSpi}.
     *
     * @param cfgVal {@code true} if {@code localHost} should be set in configuration adapter.
     * @param spiVal {@code true} if {@code localHost} should be set in SPI
     * @param exp Expected value of {@code localHost} property in SPI after injection.
     * @throws GridException If test fails.
     */
    private void processTcpCommunicationSpiTestInjection(boolean cfgVal, boolean spiVal, @Nullable String exp)
        throws GridException {
        GridResourceProcessor proc = getResourceProcessor(cfgVal);

        GridTcpCommunicationSpi spi = new GridTcpCommunicationSpi();

        if (spiVal)
            spi.setLocalAddress(SPI_LOCAL_ADDR_VALUE);

        proc.inject(spi);

        assertEquals("Invalid localAddr value after injection: ", exp, spi.getLocalAddress());
    }

    /**
     * Get test resource processor with or without {@code localHost} property set.
     *
     * @param cfgVal {@code true} if {@code localHost} property value should be set to configuration.
     * @return Resource processor.
     */
    private GridResourceProcessor getResourceProcessor(boolean cfgVal) {
        GridTestKernalContext ctx = newContext();

        if (cfgVal)
            ctx.config().setLocalHost(CONFIG_LOCAL_ADDR_VALUE);

        GridResourceProcessor proc = new GridResourceProcessor(ctx);

        proc.setSpringContext(null);

        return proc;
    }
}

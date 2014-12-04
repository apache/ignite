/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.securesession.noop;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Default no-op implementation of the secure session SPI which supports all subject types and denies any token.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has no optional configuration parameters.
 * <h2 class="header">Java Example</h2>
 * GridNoopSecureSessionSpi is used by default and has no parameters to be explicitly configured.
 * <pre name="code" class="java">
 * GridNoopSecureSessionSpi spi = new GridNoopSecureSessionSpi();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default SecureSession SPI.
 * cfg.setSecureSessionSpi(spi);
 *
 * // Start grid.
 * GridGain.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridNoopSecureSessionSpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *         ...
 *         &lt;property name="secureSessionSpi"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.SecureSession.noop.GridNoopSecureSessionSpi"/&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridSecureSessionSpi
 */
@GridSpiNoop
@GridSpiMultipleInstancesSupport(true)
public class GridNoopSecureSessionSpi extends GridSpiAdapter
    implements GridSecureSessionSpi, GridNoopSecureSessionSpiMBean {
    /** Empty bytes array. */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** Injected grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public boolean supported(GridSecuritySubjectType subjType) {
        // If this SPI is included, then session management is disabled.
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean validate(GridSecuritySubjectType subjType, UUID subjId, @Nullable byte[] tok,
        @Nullable Object params) throws GridSpiException {
        // Never validate any token - all tokens are invalid.
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte[] generateSessionToken(GridSecuritySubjectType subjType, UUID subjId,
        @Nullable Object params) {
        return EMPTY_BYTE_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridNoopSecureSessionSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNoopSecureSessionSpi.class, this);
    }
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.never;

import org.apache.ignite.cluster.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.*;

/**
 * This class provides failover SPI implementation that never fails over. This implementation
 * never fails over a failed job by always returning {@code null} out of
 * {@link GridFailoverSpi#failover(GridFailoverContext, List)} method.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has no optional configuration parameters.
 * <p>
 * Here is a Java example on how to configure grid with {@code GridNeverFailoverSpi}:
 * <pre name="code" class="java">
 * GridNeverFailoverSpi spi = new GridNeverFailoverSpi();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default failover SPI.
 * cfg.setFailoverSpiSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is an example on how to configure grid with {@code GridNeverFailoverSpi} from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="failoverSpi"&gt;
 *     &lt;bean class="org.gridgain.grid.spi.failover.never.GridNeverFailoverSpi"/&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridFailoverSpi
 */
@GridSpiMultipleInstancesSupport(true)
public class GridNeverFailoverSpi extends GridSpiAdapter implements GridFailoverSpi, GridNeverFailoverSpiMBean {
    /** Injected grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridNeverFailoverSpiMBean.class);

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
    @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> top) {
        U.warn(log, "Returning 'null' node for failed job (failover will not happen) [job=" +
            ctx.getJobResult().getJob() + ", task=" +  ctx.getTaskSession().getTaskName() +
            ", sessionId=" + ctx.getTaskSession().getId() + ']');

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNeverFailoverSpi.class, this);
    }
}

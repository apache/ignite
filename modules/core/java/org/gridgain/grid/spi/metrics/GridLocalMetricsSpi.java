// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.metrics;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;

/**
 * Grid local metrics SPI allows grid to get metrics on local VM. These metrics
 * are a subset of metrics included into {@link GridNode#metrics()} method.
 * This way every grid node can become aware of certain changes on other nodes,
 * such as CPU load for example.
 * <p>
 * GridGain comes with following implementations:
 * <ul>
 *      <li>
 *          {@link org.gridgain.grid.spi.metrics.jdk.GridJdkLocalMetricsSpi} - provides Local VM metrics.
 *      </li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link Grid#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridLocalMetricsSpi extends GridSpi, GridSpiJsonConfigurable {
    /**
     * Provides local VM metrics.
     *
     * @return Local VM metrics.
     */
    public GridLocalMetrics getMetrics();
}

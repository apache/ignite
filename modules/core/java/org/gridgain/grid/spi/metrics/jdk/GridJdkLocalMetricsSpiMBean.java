// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.metrics.jdk;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.metrics.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management MBean for {@link GridJdkLocalMetricsSpi} SPI.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides access to JDK local metrics SPI configuration.")
public interface GridJdkLocalMetricsSpiMBean extends GridLocalMetrics, GridSpiManagementMBean {
    /**
     * Checks whether file system metrics are enabled. These metrics may be expensive to get in
     * certain environments and are disabled by default.
     *
     * @return Flag indicating whether file system metrics are enabled.
     * @see GridJdkLocalMetricsSpi#setFileSystemMetricsEnabled(boolean)
     */
    @GridMBeanDescription("Flag indicating whether file system metrics are enabled.")
    public boolean isFileSystemMetricsEnabled();

    /**
     * Gets root from which file space metrics should be counted. This property only makes sense
     * if {@link #isFileSystemMetricsEnabled()} set to {@code true}.
     *
     * @return Root from which file space metrics should be counted.
     */
    @GridMBeanDescription("Root from which file space metrics should be counted.")
    public String getFileSystemRoot();

    /**
     * Returns time interval between CPU load metrics updates.
     *
     * @return CPU load metrics update frequency in milliseconds.
     */
    @GridMBeanDescription("Frequency of CPU load metrics update.")
    public long getCpuLoadUpdateFrequency();
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.apache.ignite.mbean.*;

import java.util.*;

/**
 * This interface defines basic MBean for all SPI implementations. Every SPI implementation
 * should provide implementation for this MBean interface. Note that SPI implementation can extend this
 * interface as necessary.
 */
public interface IgniteSpiManagementMBean {
    /**
     * Gets string presentation of the start timestamp.
     *
     * @return String presentation of the start timestamp.
     */
    @IgniteMBeanDescription("String presentation of the start timestamp.")
    public String getStartTimestampFormatted();

    /**
     * Gets string presentation of up-time for this SPI.
     *
     * @return String presentation of up-time for this SPI.
     */
    @IgniteMBeanDescription("String presentation of up-time for this SPI.")
    public String getUpTimeFormatted();

    /**
     * Get start timestamp of this SPI.
     *
     * @return Start timestamp of this SPI.
     */
    @IgniteMBeanDescription("Start timestamp of this SPI.")
    public long getStartTimestamp();

    /**
     * Gets up-time of this SPI in ms.
     *
     * @return Up-time of this SPI.
     */
    @IgniteMBeanDescription("Up-time of this SPI in milliseconds.")
    public long getUpTime();

    /**
     * Gets Gridgain installation home folder (i.e. ${GRIDGAIN_HOME});
     *
     * @return Gridgain installation home folder.
     */
    @IgniteMBeanDescription("Gridgain installation home folder.")
    public String getGridGainHome();

    /**
     * Gets ID of the local node.
     *
     * @return ID of the local node.
     */
    @IgniteMBeanDescription("ID of the local node.")
    public UUID getLocalNodeId();

    /**
     * Gets name of the SPI.
     *
     * @return Name of the SPI.
     */
    @IgniteMBeanDescription("Name of the SPI.")
    public String getName();
}

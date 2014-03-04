/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.util.mbean.*;
import java.util.*;

/**
 * This interface defines basic MBean for all SPI implementations. Every SPI implementation
 * should provide implementation for this MBean interface. Note that SPI implementation can extend this
 * interface as necessary.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridSpiManagementMBean {
    /**
     * Gets SPI provider's author.
     *
     * @return SPI provider's author.
     */
    @GridMBeanDescription("SPI provider's author.")
    public String getAuthor();

    /**
     * Gets vendor's URL.
     *
     * @return Vendor's URL.
     */
    @GridMBeanDescription("Vendor's URL.")
    public String getVendorUrl();

    /**
     * Gets vendor's email (info or support).
     *
     * @return Vendor's email (info or support).
     */
    @GridMBeanDescription("Vendor's email (info or support).")
    public String getVendorEmail();

    /**
     * Gets SPI implementation version.
     *
     * @return SPI implementation version.
     */
    @GridMBeanDescription("SPI implementation version.")
    public String getVersion();

    /**
     * Gets string presentation of the start timestamp.
     *
     * @return String presentation of the start timestamp.
     */
    @GridMBeanDescription("String presentation of the start timestamp.")
    public String getStartTimestampFormatted();

    /**
     * Gets string presentation of up-time for this SPI.
     *
     * @return String presentation of up-time for this SPI.
     */
    @GridMBeanDescription("String presentation of up-time for this SPI.")
    public String getUpTimeFormatted();

    /**
     * Get start timestamp of this SPI.
     *
     * @return Start timestamp of this SPI.
     */
    @GridMBeanDescription("Start timestamp of this SPI.")
    public long getStartTimestamp();

    /**
     * Gets up-time of this SPI in ms.
     *
     * @return Up-time of this SPI.
     */
    @GridMBeanDescription("Up-time of this SPI in milliseconds.")
    public long getUpTime();

    /**
     * Gets Gridgain installation home folder (i.e. ${GRIDGAIN_HOME});
     *
     * @return Gridgain installation home folder.
     */
    @GridMBeanDescription("Gridgain installation home folder.")
    public String getGridGainHome();

    /**
     * Gets ID of the local node.
     *
     * @return ID of the local node.
     */
    @GridMBeanDescription("ID of the local node.")
    public UUID getLocalNodeId();

    /**
     * Gets name of the SPI.
     *
     * @return Name of the SPI.
     */
    @GridMBeanDescription("Name of the SPI.")
    public String getName();
}

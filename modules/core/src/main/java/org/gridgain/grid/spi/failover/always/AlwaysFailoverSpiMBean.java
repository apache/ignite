/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.always;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management bean for {@link AlwaysFailoverSpi}.
 */
@IgniteMBeanDescription("MBean that provides access to always failover SPI configuration.")
public interface AlwaysFailoverSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets maximum number of attempts to execute a failed job on another node.
     * If not specified, {@link AlwaysFailoverSpi#DFLT_MAX_FAILOVER_ATTEMPTS} value will be used.
     *
     * @return Maximum number of attempts to execute a failed job on another node.
     */
    @IgniteMBeanDescription("Maximum number of attempts to execute a failed job on another node.")
    public int getMaximumFailoverAttempts();

    /**
     * Get total number of jobs that were failed over.
     *
     * @return Total number of failed over jobs.
     */
    @IgniteMBeanDescription("Total number of jobs that were failed over.")
    public int getTotalFailoverJobsCount();
}

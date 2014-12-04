/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.jobstealing;

import org.apache.ignite.mbean.*;
import org.gridgain.grid.spi.*;

/**
 * Management bean for {@link GridJobStealingFailoverSpi}.
 */
@IgniteMBeanDescription("MBean that provides access to job stealing failover SPI configuration.")
public interface GridJobStealingFailoverSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets maximum number of attempts to execute a failed job on another node.
     * If job gets stolen and thief node exists then it is not considered as
     * failed job.
     * If not specified, {@link GridJobStealingFailoverSpi#DFLT_MAX_FAILOVER_ATTEMPTS} value will be used.
     *
     * @return Maximum number of attempts to execute a failed job on another node.
     */
    @IgniteMBeanDescription("Maximum number of attempts to execute a failed job on another node.")
    public int getMaximumFailoverAttempts();

    /**
     * Get total number of jobs that were failed over including stolen ones.
     *
     * @return Total number of failed over jobs.
     */
    @IgniteMBeanDescription("Total number of jobs that were failed over including stolen ones.")
    public int getTotalFailedOverJobsCount();

    /**
     * Get total number of jobs that were stolen.
     *
     * @return Total number of stolen jobs.
     */
    @IgniteMBeanDescription("Total number of jobs that were stolen.")
    public int getTotalStolenJobsCount();
}

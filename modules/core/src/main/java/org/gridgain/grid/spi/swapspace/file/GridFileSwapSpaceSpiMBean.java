/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace.file;

import org.apache.ignite.mbean.*;
import org.gridgain.grid.spi.*;

/**
 * Management bean for {@link GridFileSwapSpaceSpi}.
 */
@IgniteMBeanDescription("MBean that provides configuration information on file-based swapspace SPI.")
public interface GridFileSwapSpaceSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets base directory.
     *
     * @return Base directory.
     */
    @IgniteMBeanDescription("Base directory.")
    public String getBaseDirectory();

    /**
     * Gets maximum sparsity.
     *
     * @return Maximum sparsity.
     */
    @IgniteMBeanDescription("Maximum sparsity.")
    public float getMaximumSparsity();

    /**
     * Gets write buffer size in bytes.
     *
     * @return Write buffer size in bytes.
     */
    @IgniteMBeanDescription("Write buffer size in bytes.")
    public int getWriteBufferSize();

    /**
     * Gets max write queue size in bytes.
     *
     * @return Max write queue size in bytes.
     */
    @IgniteMBeanDescription("Max write queue size in bytes.")
    public int getMaxWriteQueueSize();

    /**
     * Gets read pool size.
     *
     * @return Read pool size.
     */
    @IgniteMBeanDescription("Read pool size.")
    public int getReadStripesNumber();
}

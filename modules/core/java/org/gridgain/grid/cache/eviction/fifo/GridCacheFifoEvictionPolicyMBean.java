/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.fifo;

import org.gridgain.grid.util.mbean.*;

/**
 * MBean for {@code FIFO} eviction policy.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean for FIFO cache eviction policy.")
public interface GridCacheFifoEvictionPolicyMBean {
    /**
     * Gets name of metadata attribute used to store eviction policy data.
     *
     * @return Name of metadata attribute used to store eviction policy data.
     */
    @GridMBeanDescription("Name of metadata attribute used to store eviction policy data.")
    public String getMetaAttributeName();

    /**
     * Gets maximum allowed cache size.
     *
     * @return Maximum allowed cache size.
     */
    @GridMBeanDescription("Maximum allowed cache size.")
    public int getMaxSize();

    /**
     * Sets maximum allowed cache size.
     *
     * @param max Maximum allowed cache size.
     */
    @GridMBeanDescription("Set maximum allowed cache size.")
    public void setMaxSize(int max);

    /**
     * Gets current queue size.
     *
     * @return Current queue size.
     */
    @GridMBeanDescription("Current FIFO queue size.")
    public int getCurrentSize();
}

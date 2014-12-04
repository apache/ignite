/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.lru;

import org.apache.ignite.mbean.*;

/**
 * MBean for {@code LRU} eviction policy.
 */
@IgniteMBeanDescription("MBean for LRU cache eviction policy.")
public interface GridCacheLruEvictionPolicyMBean {
    /**
     * Gets name of metadata attribute used to store eviction policy data.
     *
     * @return Name of metadata attribute used to store eviction policy data.
     */
    @IgniteMBeanDescription("Name of metadata attribute used to store eviction policy data.")
    public String getMetaAttributeName();

    /**
     * Gets maximum allowed cache size.
     *
     * @return Maximum allowed cache size.
     */
    @IgniteMBeanDescription("Maximum allowed cache size.")
    public int getMaxSize();

    /**
     * Sets maximum allowed cache size.
     *
     * @param max Maximum allowed cache size.
     */
    @IgniteMBeanDescription("Sets maximum allowed cache size.")
    public void setMaxSize(int max);

    /**
     * Gets current queue size.
     *
     * @return Current queue size.
     */
    @IgniteMBeanDescription("Current queue size.")
    public int getCurrentSize();
}

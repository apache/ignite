/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.ggfs;

import org.gridgain.grid.util.mbean.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * MBean for {@code GGFS per-block LRU} eviction policy.
 */
@GridMBeanDescription("MBean for GGFS per-block LRU cache eviction policy.")
public interface GridCacheGgfsPerBlockLruEvictionPolicyMBean {
    /**
     * Gets maximum allowed size of all blocks in bytes.
     *
     * @return Maximum allowed size of all blocks in bytes.
     */
    @GridMBeanDescription("Maximum allowed size of all blocks in bytes.")
    public long getMaxSize();

    /**
     * Sets maximum allowed size of data in all blocks in bytes.
     *
     * @param maxSize Maximum allowed size of data in all blocks in bytes.
     */
    @GridMBeanDescription("Sets aximum allowed size of data in all blocks in bytes.")
    public void setMaxSize(long maxSize);

    /**
     * Gets maximum allowed amount of blocks.
     *
     * @return Maximum allowed amount of blocks.
     */
    @GridMBeanDescription("Maximum allowed amount of blocks.")
    public int getMaxBlocks();

    /**
     * Sets maximum allowed amount of blocks.
     *
     * @param maxBlocks Maximum allowed amount of blocks.
     */
    @GridMBeanDescription("Sets maximum allowed amount of blocks.")
    public void setMaxBlocks(int maxBlocks);

    /**
     * Gets collection of regex for paths whose blocks must not be evicted.
     *
     * @return Collection of regex for paths whose blocks must not be evicted.
     */
    @GridMBeanDescription("Collection of regex for paths whose blocks must not be evicted.")
    @Nullable public Collection<String> getExcludePaths();

    /**
     * Sets collection of regex for paths whose blocks must not be evicted.
     *
     * @param excludePaths Collection of regex for paths whose blocks must not be evicted.
     */
    @GridMBeanDescription("Sets collection of regex for paths whose blocks must not be evicted.")
    public void setExcludePaths(@Nullable Collection<String> excludePaths);

    /**
     * Gets current size of data in all blocks.
     *
     * @return Current size of data in all blocks.
     */
    @GridMBeanDescription("Current size of data in all blocks.")
    public long getCurrentSize();

    /**
     * Gets current amount of blocks.
     *
     * @return Current amount of blocks.
     */
    @GridMBeanDescription("Current amount of blocks.")
    public int getCurrentBlocks();
}

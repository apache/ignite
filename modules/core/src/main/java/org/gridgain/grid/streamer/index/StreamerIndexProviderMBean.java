/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.index;

import org.apache.ignite.mbean.*;
import org.jetbrains.annotations.*;

/**
 * Streamer window index provider MBean.
 */
public interface StreamerIndexProviderMBean {
    /**
     * Index name.
     *
     * @return Index name.
     */
    @IgniteMBeanDescription("Index name.")
    @Nullable public String name();

    /**
     * Gets index updater class name.
     *
     * @return Index updater class.
     */
    @IgniteMBeanDescription("Index updater class name.")
    public String updaterClass();

    /**
     * Gets index unique flag.
     *
     * @return Index unique flag.
     */
    @IgniteMBeanDescription("Index unique flag.")
    public boolean unique();

    /**
     * Returns {@code true} if index supports sorting and therefore can perform range operations.
     *
     * @return Index sorted flag.
     */
    @IgniteMBeanDescription("Index sorted flag.")
    public boolean sorted();

    /**
     * Gets index policy.
     *
     * @return Index policy.
     */
    @IgniteMBeanDescription("Index policy.")
    public StreamerIndexPolicy policy();

    /**
     * Gets current index size.
     *
     * @return Current index size.
     */
    @IgniteMBeanDescription("Current index size.")
    public int size();
}

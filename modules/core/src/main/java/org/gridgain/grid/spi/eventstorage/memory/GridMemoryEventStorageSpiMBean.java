/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.eventstorage.memory;

import org.apache.ignite.mbean.*;
import org.gridgain.grid.spi.*;

/**
 * Management bean for {@link GridMemoryEventStorageSpi}.
 * Beside properties defined for every SPI bean this one gives access to:
 * <ul>
 * <li>Event expiration time (see {@link #getExpireAgeMs()})</li>
 * <li>Maximum queue size (see {@link #getExpireCount()})</li>
 * <li>Method that removes all items from queue (see {@link #clearAll()})</li>
 * </ul>
 */
@IgniteMBeanDescription("MBean that provides access to memory event storage SPI configuration.")
public interface GridMemoryEventStorageSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets event time-to-live value. Implementation must guarantee
     * that event would not be accessible if its lifetime exceeds this value.
     *
     * @return Event time-to-live.
     */
    @IgniteMBeanDescription("Event time-to-live value.")
    public long getExpireAgeMs();

    /**
     * Gets maximum event queue size. New incoming events will oust
     * oldest ones if queue size exceeds this limit.
     *
     * @return Maximum event queue size.
     */
    @IgniteMBeanDescription("Maximum event queue size.")
    public long getExpireCount();

    /**
     * Gets current queue size of the event queue.
     *
     * @return Current queue size of the event queue.
     */
    @IgniteMBeanDescription("Current event queue size.")
    public long getQueueSize();

    /**
     * Removes all events from the event queue.
     */
    @IgniteMBeanDescription("Removes all events from the event queue.")
    public void clearAll();
}

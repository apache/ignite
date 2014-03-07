/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;

/**
 * Queue item.
 */
public interface GridCacheQueueItem<T> extends GridCacheInternal {
    /**
     * Gets item id.
     *
     * @return Item id.
     */
    public int id();

    /**
     * Gets queue id.
     *
     * @return Item id.
     */
    public String queueId();

    /**
     * Gets user object being put into queue.
     *
     * @return User object being put into queue.
     */
    public T userObject();

    /**
     * Gets sequence number.
     *
     * @return Sequence number.
     */
    public long sequence();
}

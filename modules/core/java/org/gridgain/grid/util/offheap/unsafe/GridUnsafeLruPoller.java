// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

/**
 * Polls LRU.
 *
 * @author @java.author
 * @version @java.version
 */
interface GridUnsafeLruPoller {
    /**
     * Frees space from LRU queue.
     *
     * @param size Size of the space to free.
     */
    public void lruPoll(int size);
}

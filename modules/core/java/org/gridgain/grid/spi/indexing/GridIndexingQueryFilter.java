/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

/**
 * Space name and key filter.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridIndexingQueryFilter<K, V> {
    /**
     * Applies filter.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @return {@code True} if given parameter pass filter, {@code false} otherwise.
     */
    public boolean apply(String spaceName, K key, V val);
}

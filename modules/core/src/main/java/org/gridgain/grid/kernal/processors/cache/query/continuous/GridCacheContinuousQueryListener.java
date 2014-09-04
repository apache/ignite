/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

/**
 * Continuous query listener.
 */
interface GridCacheContinuousQueryListener<K, V> {
    /**
     *
     */
    public void onExecution();

    /**
     * Entry update callback.
     *
     * @param e Entry.
     * @param recordEvt Whether to record event.
     */
    public void onEntryUpdate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt);
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.index;

import org.gridgain.grid.*;

/**
 * Represents an actual instance of an index. Used by a {@link org.gridgain.grid.streamer.StreamerWindow}
 * to perform event indexing.
 * <p>
 * To configure index for a streamer window, use
 * {@link org.gridgain.grid.streamer.window.StreamerWindowAdapter#setIndexes(GridStreamerIndexProvider[])}.
 */
public interface GridStreamerIndexProvider<E, K, V> extends GridStreamerIndexProviderMBean {
    /**
     * Gets index name.
     *
     * @return Name of the index.
     */
    public String getName();

    /**
     * Gets user view for this index. This view is a snapshot
     * of a current index state. Once returned, it does not
     * change over time.
     *
     * @return User view for this index.
     */
    public GridStreamerIndex<E, K, V> index();

    /**
     * Initializes the index.
     */
    public void initialize();

    /**
     * Resets the index to an initial empty state.
     */
    public void reset();

    /**
     * Disposes the index.
     */
    public void dispose();

    /**
     * Adds an event to index.
     *
     * @param sync Index update synchronizer.
     * @param evt Event to add to an index.
     * @throws GridException If failed to add event to an index.
     */
    public void add(GridStreamerIndexUpdateSync sync, E evt) throws GridException;

    /**
     * Removes an event from index.
     *
     * @param sync Index update synchronizer.
     * @param evt Event to remove from index.
     * @throws GridException If failed to add event to an index.
     */
    public void remove(GridStreamerIndexUpdateSync sync, E evt) throws GridException;

    /**
     * Gets event indexing policy, which defines how events
     * are tracked within an index.
     *
     * @return index policy.
     */
    public GridStreamerIndexPolicy getPolicy();

    /**
     * Checks whether this index is unique or not. If it is, equal events
     * are not allowed, which means that if a newly-added event is found
     * to be equal to one of the already present events
     * ({@link Object#equals(Object)} returns {@code true}), an exception
     * is thrown.
     *
     * @return {@code True} for unique index.
     */
    public boolean isUnique();

    /**
     * Finalizes an update operation.
     *
     * @param sync Index update synchronizer.
     * @param evt Updated event.
     * @param rollback Rollback flag. If {@code true}, a rollback was made.
     * @param rmv Remove flag. If {@code true}, the event was removed from index.
     */
    public void endUpdate(GridStreamerIndexUpdateSync sync, E evt, boolean rollback, boolean rmv);
}

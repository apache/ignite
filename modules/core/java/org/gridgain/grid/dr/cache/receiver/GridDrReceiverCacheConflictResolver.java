/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.receiver;

/**
 * Data center replication receiver cache conflict resolver. In case particular cache can be updated from multiple
 * topologies (e.g. from local topology and remote data center, or from several remote data centers, etc.), then
 * conflict resolver will be used to determine which value to pick in case conflicting topologies update the same key
 * and this conflict cannot be resolved automatically for some reason.
 * <p>
 * You can inject any resources in implementation of this interface.
 */
public interface GridDrReceiverCacheConflictResolver<K, V> {
    /**
     * Resolve conflicting key update.
     *
     * @param ctx Conflict resolution context.
     */
    public void resolve(GridDrReceiverCacheConflictContext<K, V> ctx);
}

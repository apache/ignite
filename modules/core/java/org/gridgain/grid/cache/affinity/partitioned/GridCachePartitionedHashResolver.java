// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.partitioned;

import org.gridgain.grid.*;

import java.io.*;

/**
 * Resolver which is used to provide alternate hash ID, other than node ID.
 * <p>
 * Node IDs constantly change when nodes get restarted, which causes them to be placed on different locations in the
 * hash ring, and hence causing repartitioning. Providing an alternate hash ID, which survives node restarts, puts
 * node on the same location on the hash ring, hence minimizing required repartitioning.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCachePartitionedHashResolver extends Serializable {
    /**
     * Resolve alternate hash ID for the given Grid node.
     *
     * @param node Grid node.
     * @return Resolved hash ID.
     */
    public Object resolve(GridNode node);
}

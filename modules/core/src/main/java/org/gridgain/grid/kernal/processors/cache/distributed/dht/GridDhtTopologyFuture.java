/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.discovery.*;

import java.util.*;

/**
 * Future that implements a barrier after which dht topology is safe to use. Topology is considered to be
 * safe to use when all transactions that involve moving primary partitions are completed and partition map
 * exchange is also completed.
 * <p/>
 * When new new transaction is started, it will wait for this future before acquiring new locks on particular
 * topology version.
 */
public interface GridDhtTopologyFuture extends GridFuture<Long> {
    /**
     * Gets a topology snapshot for the topology version represented by the future. Note that by the time
     * partition exchange completes some nodes from the snapshot may leave the grid. One should use discovery
     * service to check if the node is valid.
     * <p/>
     * This method will block until the topology future is ready.
     *
     * @return Topology snapshot for particular topology version.
     * @throws GridException If topology future failed.
     */
    public GridDiscoveryTopologySnapshot topologySnapshot() throws GridException;
}

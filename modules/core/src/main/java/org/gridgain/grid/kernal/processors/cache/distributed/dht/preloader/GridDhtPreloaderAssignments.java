/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Partition to node assignments.
 */
public class GridDhtPreloaderAssignments<K, V> extends
    ConcurrentHashMap<ClusterNode, GridDhtPartitionDemandMessage<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exchange future. */
    @GridToStringExclude
    private final GridDhtPartitionsExchangeFuture<K, V> exchFut;

    /** Last join order. */
    private final long topVer;

    /**
     * @param exchFut Exchange future.
     * @param topVer Last join order.
     */
    public GridDhtPreloaderAssignments(GridDhtPartitionsExchangeFuture<K, V> exchFut, long topVer) {
        assert exchFut != null;
        assert topVer > 0;

        this.exchFut = exchFut;
        this.topVer = topVer;
    }

    /**
     * @return Exchange future.
     */
    GridDhtPartitionsExchangeFuture<K, V> exchangeFuture() {
        return exchFut;
    }

    /**
     * @return Topology version.
     */
    long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPreloaderAssignments.class, this, "exchId", exchFut.exchangeId(),
            "super", super.toString());
    }
}


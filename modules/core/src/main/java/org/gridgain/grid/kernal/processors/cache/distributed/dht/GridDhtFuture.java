/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Keys to retry.
 */
public interface GridDhtFuture<T> extends IgniteFuture<T> {
    /**
     * Node that future should be able to provide keys to retry before
     * it completes, so it's not necessary to wait till future is done
     * to get retry keys.
     *
     * @return Keys to retry because this node is no longer a primary or backup.
     */
    public Collection<Integer> invalidPartitions();
}

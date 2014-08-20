/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import java.util.*;

/**
 * Determines which node should be connected when operation on a key is requested.
 * <p>
 * If implementation of data affinity implements {@link GridClientTopologyListener} interface as well,
 * then affinity will be added to topology listeners on client start before first connection is established
 * and will be removed after last connection is closed.
 */
public interface GridClientDataAffinity {
    /**
     * Gets primary affinity node for a key. In case of replicated cache all nodes are equal and can be
     * considered primary, so it may return any node. In case of partitioned cache primary node is returned.
     *
     * @param key Key to get affinity for.
     * @param nodes Nodes to choose from.
     * @return Affinity nodes for the given partition.
     */
    public GridClientNode node(Object key, Collection<? extends GridClientNode> nodes);
}

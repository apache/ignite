/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.balancer;

import org.gridgain.client.*;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Simple balancer that implements round-robin balancing.
 */
public class GridClientRoundRobinBalancer extends GridClientBalancerAdapter implements GridClientTopologyListener {
    /** Lock. */
    private Lock lock = new ReentrantLock();

    /** Nodes to share load. */
    private LinkedList<UUID> nodeQueue = new LinkedList<>();
    
    /** {@inheritDoc} */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes)
        throws GridClientException {
        assert !nodes.isEmpty();

        if (isPreferDirectNodes()) {
            Collection<GridClientNode> direct = selectDirectNodes(nodes);

            int directSize = direct.size();

            // If set of direct nodes is not empty and differ from original one
            // replace original set of nodes with directly available.
            if (directSize > 0 && directSize < nodes.size())
                nodes = direct;
        }

        Map<UUID, GridClientNode> lookup = new HashMap<>(nodes.size());

        for (GridClientNode node : nodes)
            lookup.put(node.nodeId(), node);

        lock.lock();

        try {
            GridClientNode balanced = null;

            for (Iterator<UUID> iter = nodeQueue.iterator(); iter.hasNext();) {
                UUID nodeId = iter.next();

                balanced = lookup.get(nodeId);

                if (balanced != null) {
                    iter.remove();

                    break;
                }
            }

            if (balanced != null) {
                nodeQueue.addLast(balanced.nodeId());

                return balanced;
            }

            throw new GridClientException("Passed nodes doesn't present in topology " +
                "[nodes=" + nodes + ", top=" + nodeQueue);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeAdded(GridClientNode node) {
        lock.lock();

        try {
            nodeQueue.addFirst(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(GridClientNode node) {
        lock.lock();
        
        try {
            nodeQueue.remove(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }
}

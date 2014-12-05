/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.roundrobin;

import org.apache.ignite.cluster.*;

import java.util.*;

/**
 * Load balancer for per-task configuration.
 */
class RoundRobinPerTaskLoadBalancer {
    /** Balancing nodes. */
    private ArrayDeque<ClusterNode> nodeQueue;

    /** Jobs mapped flag. */
    private volatile boolean isMapped;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Call back for job mapped event.
     */
    void onMapped() {
        isMapped = true;
    }

    /**
     * Gets balanced node for given topology. This implementation
     * is to be used only from {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method
     * and, therefore, does not need to be thread-safe.
     *
     * @param top Topology to pick from.
     * @return Best balanced node.
     */
    ClusterNode getBalancedNode(List<ClusterNode> top) {
        assert top != null;
        assert !top.isEmpty();

        boolean readjust = isMapped;

        synchronized (mux) {
            // Populate first time.
            if (nodeQueue == null)
                nodeQueue = new ArrayDeque<>(top);

            // If job has been mapped, then it means
            // that it is most likely being failed over.
            // In this case topology might have changed
            // and we need to readjust with every apply.
            if (readjust)
                // Add missing nodes.
                for (ClusterNode node : top)
                    if (!nodeQueue.contains(node))
                        nodeQueue.offer(node);

            ClusterNode next = nodeQueue.poll();

            // If jobs have been mapped, we need to make sure
            // that queued node is still in topology.
            if (readjust && next != null) {
                while (!top.contains(next) && !nodeQueue.isEmpty())
                    next = nodeQueue.poll();

                // No nodes found and queue is empty.
                if (next != null && !top.contains(next))
                    return null;
            }

            if (next != null)
                // Add to the end.
                nodeQueue.offer(next);

            return next;
        }
    }

    /**
     * THIS METHOD IS USED ONLY FOR TESTING.
     *
     * @return Internal list of nodes.
     */
    List<ClusterNode> getNodes() {
        synchronized (mux) {
            return Collections.unmodifiableList(new ArrayList<>(nodeQueue));
        }
    }
}

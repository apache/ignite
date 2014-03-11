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

/**
 * Simple balancer that relies on random node selection from a given collection. This implementation
 * has no any caches and treats each given collection as a new one.
 * <p>
 * More strictly, for any non-empty collection of size <tt>n</tt> the probability of selection of any
 * node in this collection will be <tt>1/n</tt>.
 */
public class GridClientRandomBalancer extends GridClientBalancerAdapter {
    /** Random for node selection. */
    private Random random = new Random();

    /**
     * Picks up a random node from a collection.
     *
     * @param nodes Nodes to pick from.
     * @return Random node from collection.
     */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes) {
        assert !nodes.isEmpty();

        int size = nodes.size();

        if (isPreferDirectNodes()) {
            Collection<GridClientNode> direct = selectDirectNodes(nodes);

            int directSize = direct.size();

            // If set of direct nodes is not empty and differ from original one
            // replace original set of nodes with directly available.
            if (directSize > 0 && directSize < size) {
                nodes = direct;
                size = nodes.size();
            }
        }
        
        int idx = random.nextInt(size);

        if (nodes instanceof List)
            return ((List<GridClientNode>)nodes).get(idx);
        else {
            Iterator<? extends GridClientNode> it = nodes.iterator();

            while (idx > 0) {
                it.next();

                idx--;
            }

            return it.next();
        }
    }
}

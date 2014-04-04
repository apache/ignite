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
 * Interface that defines a selection logic of a server node for a particular operation.
 * Use it to define custom load balancing logic for client. Load balancer is specified via
 * {@link GridClientConfiguration#getBalancer()} configuration property.
 * <p>
 * The following implementations are provided out of the box:
 * <ul>
 * <li>{@link GridClientRandomBalancer}</li>
 * <li>{@link GridClientRoundRobinBalancer}</li>
 * </ul>
 */
public interface GridClientLoadBalancer {
    /**
     * Gets next node for executing client command.
     *
     * @param nodes Nodes to pick from, should not be empty.
     * @return Next node to pick.
     * @throws GridClientException If balancer can't match given nodes with current topology snapshot.
     */
    public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes) throws GridClientException;
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.balancer;

import java.util.Collection;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;

/**
 * Interface that defines a selection logic of a server node for a particular operation.
 * Use it to define custom load balancing logic for client. Load balancer is specified via
 * {@link org.apache.ignite.internal.client.GridClientConfiguration#getBalancer()} configuration property.
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
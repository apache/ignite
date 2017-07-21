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

package org.apache.ignite.internal.client;

import java.util.Collection;

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
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

/**
 * Listener interface for notifying on nodes joining or leaving remote grid.
 * <p>
 * Since the topology refresh is performed in background, the listeners will not be notified
 * immediately after the node leaves grid, but the maximum time window between remote grid detects
 * node leaving and client receives topology update is {@link GridClientConfiguration#getTopologyRefreshFrequency()}.
 */
public interface GridClientTopologyListener {
    /**
     * Callback for new nodes joining the remote grid.
     *
     * @param node New remote node.
     */
    public void onNodeAdded(GridClientNode node);

    /**
     * Callback for nodes leaving the remote grid.
     *
     * @param node Left node.
     */
    public void onNodeRemoved(GridClientNode node);
}
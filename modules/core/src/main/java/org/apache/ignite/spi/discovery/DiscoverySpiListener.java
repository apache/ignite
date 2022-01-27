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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Listener for grid node discovery events. See
 * {@link DiscoverySpi} for information on how grid nodes get discovered.
 */
public interface DiscoverySpiListener {
    /**
     *  Notification of local node initialization. At the time this method is called, it is guaranteed that
     *  local node consistent ID is available, but the discovery process is not started yet.
     *  This method should not block for a long time since it blocks discovery.
     *
     * @param locNode Initialized local node.
     */
    public void onLocalNodeInitialized(ClusterNode locNode);

    /**
     * Notification for grid node discovery events.
     *
     * @param notification Discovery notification object.
     *
     * @return A future that will be completed when notification process has finished.
     */
    public IgniteFuture<?> onDiscovery(DiscoveryNotification notification);
}

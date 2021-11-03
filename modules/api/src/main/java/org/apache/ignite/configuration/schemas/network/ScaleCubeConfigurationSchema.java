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

package org.apache.ignite.configuration.schemas.network;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

/**
 * ScaleCube configuration.
 */
@Config
public class ScaleCubeConfigurationSchema {
    /**
     * This multiplier is used to calculate the timeout after which the node is considered dead. For more information see
     * io.scalecube.cluster.ClusterMath#suspicionTimeout.
     */
    @Value(hasDefault = true)
    public final int membershipSuspicionMultiplier = 1;

    /**
     * Number of members to be randomly selected by a cluster node for an indirect ping request.
     */
    @Value(hasDefault = true)
    public final int failurePingRequestMembers = 1;

    /**
     * Gossip spreading interval.
     *
     * @see <a href="https://en.wikipedia.org/wiki/Gossip_protocol">Gossip Protocol</a>
     */
    @Value(hasDefault = true)
    public final int gossipInterval = 10;
}

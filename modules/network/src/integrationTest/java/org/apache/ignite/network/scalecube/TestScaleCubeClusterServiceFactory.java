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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.ClusterConfig;
import org.apache.ignite.configuration.schemas.network.ClusterMembershipView;

/**
 * Scalecube test factory. Provides fast detection time.
 */
public class TestScaleCubeClusterServiceFactory extends ScaleCubeClusterServiceFactory {
    /** {@inheritDoc} */
    @Override
    protected ClusterConfig clusterConfig(ClusterMembershipView unused) {
        return ClusterConfig.defaultLocalConfig()
                // Theoretical suspicious timeout for 5 node cluster: 500 * 1 * log(5) = 349ms
                // Short sync interval is required for faster convergence on node restarts.
                .membership(opts -> opts.syncInterval(1000).suspicionMult(1))
                // Theoretical upper bound for detection of faulty node by some other node: 500 * (e / (e - 1)) = 790ms
                .failureDetector(opts -> opts.pingInterval(500).pingReqMembers(1))
                .gossip(opts -> opts.gossipInterval(10));
    }
}

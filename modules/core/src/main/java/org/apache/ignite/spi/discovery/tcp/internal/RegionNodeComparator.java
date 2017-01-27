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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Comparator;

/**
 * New behavior for node ordering. Sort firstly by region ID, and secondly by topology version.
 * Node without region ID is less the node with the region ID.
 */
public class RegionNodeComparator implements Comparator<TcpDiscoveryNode> {
    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(TcpDiscoveryNode firstNode, TcpDiscoveryNode secondNode) {
        Long firstId = firstNode.getClusterRegionId();
        Long secondId = secondNode.getClusterRegionId();

        if ((firstId == secondId) || firstId.equals(secondId))
            return firstNode.compareTo(secondNode);
        else {
            if (firstId == null)
                return -1;
            else if (secondId == null)
                return 1;
            else return (firstId > secondId) ? 1 : -1;
        }
    }
}

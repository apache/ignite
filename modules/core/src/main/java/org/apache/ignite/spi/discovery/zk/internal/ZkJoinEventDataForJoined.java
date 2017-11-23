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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 */
class ZkJoinEventDataForJoined implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final List<ZookeeperClusterNode> top;

    /** */
    private final Map<Integer, Serializable> discoData;

    /**
     * @param top Topology.
     * @param discoData Discovery data.
     */
    ZkJoinEventDataForJoined(List<ZookeeperClusterNode> top, Map<Integer, Serializable> discoData) {
        this.top = top;
        this.discoData = discoData;
    }

    /**
     * @return Current topology.
     */
    List<ZookeeperClusterNode> topology() {
        return top;
    }

    /**
     * @return Discovery data.
     */
    Map<Integer, Serializable> discoveryData() {
        return discoData;
    }
}

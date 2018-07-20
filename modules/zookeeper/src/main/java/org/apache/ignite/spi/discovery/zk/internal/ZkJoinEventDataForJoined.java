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
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkJoinEventDataForJoined implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final List<ZookeeperClusterNode> top;

    /** */
    private final Map<Long, byte[]> discoData;

    /** */
    private final Map<Long, Long> dupDiscoData;

    /**
     * @param top Topology.
     * @param discoData Discovery data.
     */
    ZkJoinEventDataForJoined(List<ZookeeperClusterNode> top, Map<Long, byte[]> discoData, @Nullable Map<Long, Long> dupDiscoData) {
        assert top != null;
        assert discoData != null && !discoData.isEmpty();

        this.top = top;
        this.discoData = discoData;
        this.dupDiscoData = dupDiscoData;
    }

    byte[] discoveryDataForNode(long nodeOrder) {
        assert discoData != null;

        byte[] dataBytes = discoData.get(nodeOrder);

        if (dataBytes != null)
            return dataBytes;

        assert dupDiscoData != null;

        Long dupDataNode = dupDiscoData.get(nodeOrder);

        assert dupDataNode != null;

        dataBytes = discoData.get(dupDataNode);

        assert dataBytes != null;

        return dataBytes;
    }

    /**
     * @return Current topology.
     */
    List<ZookeeperClusterNode> topology() {
        assert top != null;

        return top;
    }
}

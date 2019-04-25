/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
class ZkBulkJoinContext {
    /** */
    List<T2<ZkJoinedNodeEvtData, Map<Integer, Serializable>>> nodes;

    /**
     * @param nodeEvtData Node event data.
     * @param discoData Discovery data for node.
     */
    void addJoinedNode(ZkJoinedNodeEvtData nodeEvtData, Map<Integer, Serializable> discoData) {
        if (nodes == null)
            nodes = new ArrayList<>();

        nodes.add(new T2<>(nodeEvtData, discoData));
    }

    /**
     * @return Number of joined nodes.
     */
    int nodes() {
        return nodes != null ? nodes.size() : 0;
    }
}

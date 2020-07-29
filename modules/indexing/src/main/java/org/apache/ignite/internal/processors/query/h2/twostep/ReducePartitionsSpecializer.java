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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.util.IntArray;

/**
 * Reducer partitions specializer.
 */
public class ReducePartitionsSpecializer implements C2<ClusterNode, Message, Message> {
    /** Partitions map. */
    private final Map<ClusterNode, IntArray> partsMap;

    /**
     * @param partsMap Partitions map.
     */
    public ReducePartitionsSpecializer(Map<ClusterNode, IntArray> partsMap) {
        this.partsMap = partsMap;
    }

    /** {@inheritDoc} */
    @Override public Message apply(ClusterNode node, Message msg) {
        if (msg instanceof GridH2QueryRequest) {
            GridH2QueryRequest rq = new GridH2QueryRequest((GridH2QueryRequest)msg);

            rq.queryPartitions(GridReduceQueryExecutor.toArray(partsMap.get(node)));

            return rq;
        } else if (msg instanceof GridH2DmlRequest) {
            GridH2DmlRequest rq = new GridH2DmlRequest((GridH2DmlRequest)msg);

            rq.queryPartitions(GridReduceQueryExecutor.toArray(partsMap.get(node)));

            return rq;
        }

        return msg;
    }
}

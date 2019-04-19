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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Map query info.
 */
public class MapH2QueryInfo extends H2QueryInfo {
    /** Node. */
    private final ClusterNode node;

    /** Request id. */
    private final long reqId;

    /** Segment. */
    private final int segment;

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param node Originator node ID
     * @param reqId Request ID.
     * @param segment Segment.
     */
    public MapH2QueryInfo(PreparedStatement stmt, String sql,
        ClusterNode node, long reqId, int segment) {
        super(QueryType.MAP, stmt, sql);

        this.node = node;
        this.reqId= reqId;
        this.segment = segment;
    }

    /** {@inheritDoc} */
    @Override protected void printInfo(StringBuilder msg) {
        msg.append(", node=").append(node)
            .append(", reqId=").append(reqId)
            .append(", segment=").append(segment);
    }
}

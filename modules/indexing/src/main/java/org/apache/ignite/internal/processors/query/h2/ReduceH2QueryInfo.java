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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Reduce query info.
 */
public class ReduceH2QueryInfo extends H2QueryInfo {
    /** Request id. */
    private final long reqId;

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param nodeId Originator node id.
     * @param qryId Query id.
     * @param reqId Request ID.
     * @param lbl Query label.
     */
    public ReduceH2QueryInfo(PreparedStatement stmt, String sql, UUID nodeId, long qryId, long reqId, @Nullable String lbl) {
        super(QueryType.REDUCE, stmt, sql, nodeId, qryId, lbl);

        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override protected void printInfo(StringBuilder msg) {
        msg.append(", reqId=").append(reqId);
    }
}

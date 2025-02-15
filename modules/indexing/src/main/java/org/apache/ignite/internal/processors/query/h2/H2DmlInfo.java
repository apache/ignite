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

import java.util.UUID;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.running.RunningQueryManager;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class H2DmlInfo implements TrackableQuery {
    /** Begin timestamp. */
    private final long beginTs;

    /** Query id. */
    private final long qryId;

    /** Initiator node id. */
    private final UUID initNodeId;

    /** Schema name. */
    private final String schema;

    /** Dml command. */
    private final String sql;

    /** Query label. */
    private final String lbl;

    /**
     * @param beginTs Begin timestamp.
     * @param qryId Query id.
     * @param initNodeId Initiator node id.
     * @param schema Schema name.
     * @param sql Dml command.
     * @param lbl Query label.
     */
    public H2DmlInfo(long beginTs, long qryId, UUID initNodeId, String schema, String sql, @Nullable String lbl) {
        this.beginTs = beginTs;
        this.qryId = qryId;
        this.initNodeId = initNodeId;
        this.schema = schema;
        this.sql = sql;
        this.lbl = lbl;
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /** {@inheritDoc} */
    @Override public String queryInfo(@Nullable String additionalInfo) {
        StringBuilder msgSb = new StringBuilder();

        if (qryId == RunningQueryManager.UNDEFINED_QUERY_ID)
            msgSb.append(" [globalQueryId=(undefined), node=").append(initNodeId);
        else
            msgSb.append(" [globalQueryId=").append(QueryUtils.globalQueryId(initNodeId, qryId));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", duration=").append(time()).append("ms")
            .append(", type=DML")
            .append(", schema=").append(schema)
            .append(", sql='").append(sql);

        if (lbl != null)
            msgSb.append("', label='").append(lbl);

        msgSb.append("']");

        return msgSb.toString();
    }
}

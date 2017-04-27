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

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for cache query tasks.
 */
public class VisorQueryResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node where query executed. */
    private UUID resNodeId;

    /** Query ID to store in node local. */
    private String qryId;

    /** Query columns descriptors. */
    private List<VisorQueryField> cols;

    /** Rows fetched from query. */
    private List<Object[]> rows;

    /** Whether query has more rows to fetch. */
    private boolean hasMore;

    /** Query duration */
    private long duration;

    /**
     * Default constructor.
     */
    public VisorQueryResult() {
        // No-op.
    }

    /**
     * @param resNodeId Node where query executed.
     * @param qryId Query ID for future extraction in nextPage() access.
     * @param cols Columns descriptors.
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     * @param duration Query duration.
     */
    public VisorQueryResult(
        UUID resNodeId,
        String qryId,
        List<VisorQueryField> cols,
        List<Object[]> rows,
        boolean hasMore,
        long duration
    ) {
        this.resNodeId = resNodeId;
        this.qryId = qryId;
        this.cols = cols;
        this.rows = rows;
        this.hasMore = hasMore;
        this.duration = duration;
    }

    /**
     * @return Response node id.
     */
    public UUID getResponseNodeId() {
        return resNodeId;
    }

    /**
     * @return Query id.
     */
    public String getQueryId() {
        return qryId;
    }

    /**
     * @return Columns.
     */
    public Collection<VisorQueryField> getColumns() {
        return cols;
    }

    /**
     * @return Rows fetched from query.
     */
    public List<Object[]> getRows() {
        return rows;
    }

    /**
     * @return Whether query has more rows to fetch.
     */
    public boolean isHasMore() {
        return hasMore;
    }

    /**
     * @return Duration of next page fetching.
     */
    public long getDuration() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, resNodeId);
        U.writeString(out, qryId);
        U.writeCollection(out, cols);
        U.writeCollection(out, rows);
        out.writeBoolean(hasMore);
        out.writeLong(duration);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        resNodeId = U.readUuid(in);
        qryId = U.readString(in);
        cols = U.readList(in);
        rows = U.readList(in);
        hasMore = in.readBoolean();
        duration = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResult.class, this);
    }
}

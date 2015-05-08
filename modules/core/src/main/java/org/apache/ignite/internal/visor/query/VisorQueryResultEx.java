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

import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Result for cache query tasks.
 */
public class VisorQueryResultEx extends VisorQueryResult {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node where query executed. */
    private final UUID resNodeId;

    /** Query ID to store in node local. */
    private final String qryId;

    /** Query columns descriptors. */
    private final VisorQueryField[] colNames;

    /**
     * @param resNodeId Node where query executed.
     * @param qryId Query ID for future extraction in nextPage() access.
     * @param colNames Columns types and names.
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     * @param duration Query duration.
     */
    public VisorQueryResultEx(
        UUID resNodeId,
        String qryId,
        VisorQueryField[] colNames,
        List<Object[]> rows,
        Boolean hasMore,
        long duration
    ) {
        super(rows, hasMore, duration);

        this.resNodeId = resNodeId;
        this.qryId = qryId;
        this.colNames = colNames;
    }

    /**
     * @return Response node id.
     */
    public UUID responseNodeId() {
        return resNodeId;
    }

    /**
     * @return Query id.
     */
    public String queryId() {
        return qryId;
    }

    /**
     * @return Columns names.
     */
    public VisorQueryField[] columnNames() {
        return colNames;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResultEx.class, this);
    }
}

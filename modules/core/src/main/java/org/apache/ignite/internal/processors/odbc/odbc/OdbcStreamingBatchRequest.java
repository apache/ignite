package org.apache.ignite.internal.processors.odbc.odbc;

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

import java.util.List;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute request with the batch of parameters.
 */
public class OdbcStreamingBatchRequest extends OdbcRequest implements java.lang.Comparable<OdbcStreamingBatchRequest> {
    /** Schema name. */
    @GridToStringInclude(sensitive = true)
    private String schemaName;

    /** Sql query. */
    @GridToStringExclude()
    private List<OdbcQuery> queries;

    /**
     * Last stream batch flag - whether open streamers on current connection
     * must be flushed and closed after this batch.
     */
    @GridToStringInclude(sensitive = true)
    private boolean last;

    /** Order. */
    @GridToStringInclude(sensitive = true)
    private long order;

    /**
     * @param schema Schema.
     * @param queries SQL queries list.
     * @param last Last page flag.
     * @param order Order.
     */
    public OdbcStreamingBatchRequest(@Nullable String schema, List<OdbcQuery> queries, boolean last, long order) {
        super(STREAMING_BATCH);

        this.schemaName = schema;
        this.queries = queries;
        this.last = last;
        this.order = order;
    }

    /**
     * @return Schema name.
     */
    @Nullable public String schemaName() {
        return schemaName;
    }

    /**
     * @return Queries.
     */
    public List<OdbcQuery> queries() {
        return queries;
    }

    /**
     * @return Last stream batch flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Request order.
     */
    public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcStreamingBatchRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull OdbcStreamingBatchRequest o) {
        return Long.compare(order, o.order);
    }
}

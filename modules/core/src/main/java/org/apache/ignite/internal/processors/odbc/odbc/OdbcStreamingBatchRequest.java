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

package org.apache.ignite.internal.processors.odbc.odbc;

import java.util.List;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC streaming batch request.
 */
public class OdbcStreamingBatchRequest extends OdbcRequest {
    /** Schema name. */
    private String schemaName;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private List<OdbcQuery> queries;

    /**
     * Last stream batch flag - whether open streamers on current connection
     * must be flushed and closed after this batch.
     */
    private boolean last;

    /**
     * Default constructor.
     */
    public OdbcStreamingBatchRequest() {
        this(STREAMING_BATCH);
    }

    /**
     * Constructor for child requests.
     * @param type Request type/
     */
    protected OdbcStreamingBatchRequest(byte type) {
        super(type);
    }

    /**
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param last {@code true} in case the request is the last batch at the stream.
     */
    public OdbcStreamingBatchRequest(String schemaName, List<OdbcQuery> queries, boolean last) {
        this(STREAMING_BATCH, schemaName, queries, last);
    }

    /**
     * Constructor for child requests.
     *
     * @param type Request type.
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param last {@code true} in case the request is the last batch at the stream.
     */
    protected OdbcStreamingBatchRequest(byte type, String schemaName, List<OdbcQuery> queries, boolean last) {
        super(type);

        assert last || !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.last = last;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcStreamingBatchRequest.class, this);
    }
}

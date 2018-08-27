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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute request with the batch of parameters.
 */
public class OdbcQueryExecuteOrderedBatchRequest extends OdbcQueryExecuteBatchRequest {
    /** Order. */
    @GridToStringInclude(sensitive = true)
    private long order;

    /**
     * @param schema Schema.
     * @param sqlQry SQL query.
     * @param last Last page flag.
     * @param args Arguments list.
     * @param timeout Timeout in seconds.
     * @param order Order.
     */
    public OdbcQueryExecuteOrderedBatchRequest(@Nullable String schema, String sqlQry, boolean last, Object[][] args,
        int timeout, long order) {
        super(schema, sqlQry, last, args, timeout);

        this.order = order;
    }

    /**
     * @return Request order.
     */
    public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryExecuteOrderedBatchRequest.class, this);
    }
}

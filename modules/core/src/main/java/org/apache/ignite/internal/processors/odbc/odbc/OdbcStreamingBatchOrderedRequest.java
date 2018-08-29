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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute request with the batch of parameters.
 */
public class OdbcStreamingBatchOrderedRequest extends OdbcStreamingBatchRequest {
    /** Order. */
    @GridToStringInclude(sensitive = true)
    private long order;

    /**
     * @param schema Schema.
     * @param queries SQL queries list.
     * @param last Last page flag.
     * @param order Order.
     */
    public OdbcStreamingBatchOrderedRequest(@Nullable String schema, List<OdbcQuery> queries, boolean last,
        long order) {
        super(STREAMING_BATCH_ORDERED, schema, queries, last);

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
        return S.toString(OdbcStreamingBatchOrderedRequest.class, this);
    }
}

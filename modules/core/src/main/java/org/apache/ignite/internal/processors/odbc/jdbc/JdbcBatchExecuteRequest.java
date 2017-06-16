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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener query execute request.
 */
public class JdbcBatchExecuteRequest extends JdbcRequest {
    /** Cache name. */
    private String schema;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private JdbcQuery[] queries;

    /**
     */
    public JdbcBatchExecuteRequest() {
        super(BATCH_EXEC);
    }

    /**
     * @param queries Queries.
     */
    public JdbcBatchExecuteRequest(JdbcQuery[] queries) {
        super(BATCH_EXEC);

        assert !F.isEmpty(queries);

        this.queries = queries;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String schema() {
        return schema;
    }

    /**
     * @return Sql query arguments.
     */
    public JdbcQuery[] queries() {
        return queries;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeInt(queries.length);

        for (JdbcQuery q : queries)
            q.writeBinary(writer);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        int n = reader.readInt();

        queries = new JdbcQuery[n];

        for (int i = 0; i < n; ++i) {
            queries[i] = new JdbcQuery();

            queries[i].readBinary(reader);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBatchExecuteRequest.class, this);
    }
}

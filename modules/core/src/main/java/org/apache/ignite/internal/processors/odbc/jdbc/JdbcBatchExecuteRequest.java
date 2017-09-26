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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC batch execute request.
 */
public class JdbcBatchExecuteRequest extends JdbcRequest {
    /** Schema name. */
    private String schemaName;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private List<JdbcQuery> queries;

    /** Query ID. */
    private long qryId;

    /**
     * Default constructor.
     */
    public JdbcBatchExecuteRequest() {
        super(BATCH_EXEC);
    }

    /**
     * @param qryId Query ID used to cancel batch.
     * @param schemaName Schema name.
     * @param queries Queries.
     */
    public JdbcBatchExecuteRequest(long qryId, String schemaName, List<JdbcQuery> queries) {
        super(BATCH_EXEC);

        assert !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.qryId = qryId;
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
    public List<JdbcQuery> queries() {
        return queries;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(schemaName);
        writer.writeInt(queries.size());

        for (JdbcQuery q : queries)
            q.writeBinary(writer);

        writer.writeLong(qryId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        schemaName = reader.readString();

        int n = reader.readInt();

        queries = new ArrayList<>(n);

        for (int i = 0; i < n; ++i) {
            JdbcQuery qry = new JdbcQuery();

            qry.readBinary(reader);

            queries.add(qry);
        }

        try {
            qryId = reader.available() >= 8 ? reader.readLong() : -1L;
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBatchExecuteRequest.class, this);
    }
}

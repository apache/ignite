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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
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

    /**
     * Last stream batch flag - whether open streamers on current connection
     * must be flushed and closed after this batch.
     */
    private boolean lastStreamBatch;

    /** Query ID. */
    private long qryId;

    /** Query timeout. */
    private int timeout;

    /**
     * Default constructor.
     */
    public JdbcBatchExecuteRequest() {
        super(BATCH_EXEC);
    }

    /**
     * Constructor for child requests.
     * @param type Request type/
     */
    protected JdbcBatchExecuteRequest(byte type) {
        super(type);
    }

    /**
     * @param qryId Query ID.
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param timeout Query timeout;
     * @param lastStreamBatch {@code true} in case the request is the last batch at the stream.
     */
    public JdbcBatchExecuteRequest(
        long qryId,
        String schemaName,
        List<JdbcQuery> queries,
        int timeout,
        boolean lastStreamBatch) {
        super(BATCH_EXEC);

        assert lastStreamBatch || !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.lastStreamBatch = lastStreamBatch;
        this.qryId = qryId;
        this.timeout = timeout;
    }

    /**
     * Constructor for child requests.
     *
     * @param type Request type.
     * @param qryId Query ID.
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param timeout Query timeout.
     * @param lastStreamBatch {@code true} in case the request is the last batch at the stream.
     */
    protected JdbcBatchExecuteRequest(
        byte type,
        long qryId,
        String schemaName,
        List<JdbcQuery> queries,
        int timeout,
        boolean lastStreamBatch) {
        super(type);

        assert lastStreamBatch || !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.lastStreamBatch = lastStreamBatch;
        this.qryId = qryId;
        this.timeout = timeout;
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
     * @return Last stream batch flag.
     */
    public boolean isLastStreamBatch() {
        return lastStreamBatch;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @return Query timeout.
     */
    public int timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) {
        super.writeBinary(writer, ver);

        writer.writeString(schemaName);

        if (!F.isEmpty(queries)) {
            writer.writeInt(queries.size());

            for (JdbcQuery q : queries)
                q.writeBinary(writer, ver);
        }
        else
            writer.writeInt(0);

        writer.writeBoolean(lastStreamBatch);

        if (JdbcUtils.isQueryCancelSupported(ver)) {
            writer.writeLong(qryId);
            writer.writeInt(timeout);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) {
        super.readBinary(reader, ver);

        schemaName = reader.readString();

        int n = reader.readInt();

        queries = new ArrayList<>(n);

        for (int i = 0; i < n; ++i) {
            JdbcQuery qry = new JdbcQuery();

            qry.readBinary(reader, ver);

            queries.add(qry);
        }

        try {
            if (reader.available() > 0)
                lastStreamBatch = reader.readBoolean();
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }

        if (JdbcUtils.isQueryCancelSupported(ver)) {
            qryId = reader.readLong();
            timeout = reader.readInt();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBatchExecuteRequest.class, this);
    }
}

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

    /** Client auto commit flag state. */
    private boolean autoCommit;

    /**
     * Last stream batch flag - whether open streamers on current connection
     * must be flushed and closed after this batch.
     */
    private boolean lastStreamBatch;

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
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param autoCommit Client auto commit flag state.
     * @param lastStreamBatch {@code true} in case the request is the last batch at the stream.
     */
    public JdbcBatchExecuteRequest(String schemaName, List<JdbcQuery> queries, boolean autoCommit,
        boolean lastStreamBatch) {
        super(BATCH_EXEC);

        assert lastStreamBatch || !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.autoCommit = autoCommit;
        this.lastStreamBatch = lastStreamBatch;
    }

    /**
     * Constructor for child requests.
     *
     * @param type Request type.
     * @param schemaName Schema name.
     * @param queries Queries.
     * @param autoCommit Client auto commit flag state.
     * @param lastStreamBatch {@code true} in case the request is the last batch at the stream.
     */
    protected JdbcBatchExecuteRequest(byte type, String schemaName, List<JdbcQuery> queries, boolean autoCommit,
        boolean lastStreamBatch) {
        super(type);

        assert lastStreamBatch || !F.isEmpty(queries);

        this.schemaName = schemaName;
        this.queries = queries;
        this.autoCommit = autoCommit;
        this.lastStreamBatch = lastStreamBatch;
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
     * @return Auto commit flag.
     */
    boolean autoCommit() {
        return autoCommit;
    }

    /**
     * @return Last stream batch flag.
     */
    public boolean isLastStreamBatch() {
        return lastStreamBatch;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeString(schemaName);

        if (!F.isEmpty(queries)) {
            writer.writeInt(queries.size());

            for (JdbcQuery q : queries)
                q.writeBinary(writer, protoCtx);

        }
        else
            writer.writeInt(0);

        if (protoCtx.isStreamingSupported())
            writer.writeBoolean(lastStreamBatch);

        if (protoCtx.isAutoCommitSupported())
            writer.writeBoolean(autoCommit);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        schemaName = reader.readString();

        int n = reader.readInt();

        queries = new ArrayList<>(n);

        for (int i = 0; i < n; ++i) {
            JdbcQuery qry = new JdbcQuery();

            qry.readBinary(reader, protoCtx);

            queries.add(qry);
        }

        if (protoCtx.isStreamingSupported())
            lastStreamBatch = reader.readBoolean();

        if (protoCtx.isAutoCommitSupported())
            autoCommit = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBatchExecuteRequest.class, this, super.toString());
    }
}

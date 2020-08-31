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
import java.sql.Statement;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC query execute request.
 */
public class JdbcQueryExecuteRequest extends JdbcRequest {
    /** Schema name. */
    private String schemaName;

    /** Fetch size. */
    private int pageSize;

    /** Max rows. */
    private int maxRows;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private String sqlQry;

    /** Sql query arguments. */
    @GridToStringInclude(sensitive = true)
    private Object[] args;

    /** Expected statement type. */
    private JdbcStatementType stmtType;

    /** Client auto commit flag state. */
    private boolean autoCommit;

    /** Flag, that signals, that query expects partition response in response. */
    private boolean partResReq;

    /** Explicit timeout. */
    private boolean explicitTimeout;

    /** */
    JdbcQueryExecuteRequest() {
        super(QRY_EXEC);

        autoCommit = true;
    }

    /**
     * @param stmtType Expected statement type.
     * @param schemaName Cache name.
     * @param pageSize Fetch size.
     * @param maxRows Max rows.
     * @param autoCommit Connection auto commit flag state.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public JdbcQueryExecuteRequest(JdbcStatementType stmtType, String schemaName, int pageSize, int maxRows,
        boolean autoCommit, boolean explicitTimeout, String sqlQry, Object[] args) {
        super(QRY_EXEC);

        this.schemaName = F.isEmpty(schemaName) ? null : schemaName;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.sqlQry = sqlQry;
        this.args = args;
        this.stmtType = stmtType;
        this.autoCommit = autoCommit;
        this.explicitTimeout = explicitTimeout;
    }

    /**
     * @return Fetch size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Max rows.
     */
    public int maxRows() {
        return maxRows;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /**
     * @return Sql query arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Schema name.
     */
    @Nullable public String schemaName() {
        return schemaName;
    }

    /**
     * @return Expected statement type.
     */
    public JdbcStatementType expectedStatementType() {
        return stmtType;
    }

    /**
     * @return Auto commit flag.
     */
    boolean autoCommit() {
        return autoCommit;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeString(schemaName);
        writer.writeInt(pageSize);
        writer.writeInt(maxRows);
        writer.writeString(sqlQry);

        writer.writeInt(args == null ? 0 : args.length);

        if (args != null) {
            for (Object arg : args)
                JdbcUtils.writeObject(writer, arg, protoCtx);
        }

        if (protoCtx.isAutoCommitSupported())
            writer.writeBoolean(autoCommit);

        writer.writeByte((byte)stmtType.ordinal());

        if (protoCtx.isAffinityAwarenessSupported())
            writer.writeBoolean(partResReq);

        if (protoCtx.features().contains(JdbcThinFeature.QUERY_TIMEOUT))
            writer.writeBoolean(explicitTimeout);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        schemaName = reader.readString();
        pageSize = reader.readInt();
        maxRows = reader.readInt();
        sqlQry = reader.readString();

        int argsNum = reader.readInt();

        args = new Object[argsNum];

        for (int i = 0; i < argsNum; ++i)
            args[i] = JdbcUtils.readObject(reader, protoCtx);

        if (protoCtx.isAutoCommitSupported())
            autoCommit = reader.readBoolean();

        try {
            if (reader.available() > 0)
                stmtType = JdbcStatementType.fromOrdinal(reader.readByte());
            else
                stmtType = JdbcStatementType.ANY_STATEMENT_TYPE;
        }
        catch (IOException e) {
            throw new BinaryObjectException(e);
        }

        if (protoCtx.isAffinityAwarenessSupported())
            partResReq = reader.readBoolean();

        if (protoCtx.features().contains(JdbcThinFeature.QUERY_TIMEOUT))
            explicitTimeout = reader.readBoolean();
    }

    /**
     * @return Partition response request.
     */
    public boolean partitionResponseRequest() {
        return partResReq;
    }

    /**
     * @param partResReq New partition response request.
     */
    public void partitionResponseRequest(boolean partResReq) {
        this.partResReq = partResReq;
    }

    /**
     * @return {@code true} if the query timeout is set explicitly by {@link Statement#setQueryTimeout(int)}.
     * Otherwise returns {@code false}.
     */
    public boolean explicitTimeout() {
        return explicitTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteRequest.class, this, super.toString());
    }
}

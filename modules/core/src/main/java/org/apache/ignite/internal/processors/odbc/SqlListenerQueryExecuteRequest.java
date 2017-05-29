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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener query execute request.
 */
public class SqlListenerQueryExecuteRequest extends SqlListenerRequest {
    /** Cache name. */
    private String cacheName;

    /** Fetch size. */
    private int pageSize;

    /** Max rows. */
    private int maxRows;

    /** Flag to add metadata information to response. */
    private boolean metaInResp;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private String sqlQry;

    /** Sql query arguments. */
    @GridToStringExclude
    private Object[] args;

    /**
     */
    public SqlListenerQueryExecuteRequest() {
        super(QRY_EXEC);
    }

    /**
     * @param cacheName Cache name.
     * @param pageSize Fetch size.
     * @param maxRows Max rows.
     * @param metaInResp Flag to add metadata information to response.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public SqlListenerQueryExecuteRequest(String cacheName, int pageSize, int maxRows, boolean metaInResp, String sqlQry,
        Object[] args) {
        super(QRY_EXEC);

        this.cacheName = F.isEmpty(cacheName) ? null : cacheName;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.metaInResp = metaInResp;
        this.sqlQry = sqlQry;
        this.args = args;
    }

    /**
     * @return Fetch size.
     */
    public int fetchSize() {
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
     * @return Flag to add metadata information to response.
     */
    public boolean metadataInResponse() {
        return metaInResp;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        super.writeBinary(writer, objWriter);

        writer.writeString(cacheName);
        writer.writeInt(pageSize);
        writer.writeInt(maxRows);
        writer.writeBoolean(metaInResp); // metaInResponse == false for JDBC. Query metadata is gathered on demand.
        writer.writeString(sqlQry);
        writer.writeInt(args == null ? 0 : args.length);

        if (args != null) {
            for (Object arg : args)
                objWriter.writeObject(writer, arg);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        super.readBinary(reader, objReader);

        cacheName = reader.readString();
        pageSize = reader.readInt();
        maxRows = reader.readInt();
        metaInResp = reader.readBoolean();
        sqlQry = reader.readString();

        int argsNum = reader.readInt();

        args = new Object[argsNum];

        for (int i = 0; i < argsNum; ++i)
            args[i] = objReader.readObject(reader);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlListenerQueryExecuteRequest.class, this, "args", args, true);
    }
}

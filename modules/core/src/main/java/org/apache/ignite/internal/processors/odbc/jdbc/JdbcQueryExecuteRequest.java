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
import org.apache.ignite.internal.processors.odbc.SqlListenerUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * SQL listener query execute request.
 */
public class JdbcQueryExecuteRequest extends JdbcRequest {
    /** Cache name. */
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

    /**
     */
    public JdbcQueryExecuteRequest() {
        super(QRY_EXEC);
    }

    /**
     * @param schemaName Cache name.
     * @param pageSize Fetch size.
     * @param maxRows Max rows.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public JdbcQueryExecuteRequest(String schemaName, int pageSize, int maxRows, String sqlQry,
        Object[] args) {
        super(QRY_EXEC);

        this.schemaName = F.isEmpty(schemaName) ? null : schemaName;
        this.pageSize = pageSize;
        this.maxRows = maxRows;
        this.sqlQry = sqlQry;
        this.args = args;
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
     * @return Cache name.
     */
    @Nullable public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(schemaName);
        writer.writeInt(pageSize);
        writer.writeInt(maxRows);
        writer.writeString(sqlQry);

        writer.writeInt(args == null ? 0 : args.length);

        if (args != null) {
            for (Object arg : args)
                SqlListenerUtils.writeObject(writer, arg, false);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        schemaName = reader.readString();
        pageSize = reader.readInt();
        maxRows = reader.readInt();
        sqlQry = reader.readString();

        int argsNum = reader.readInt();

        args = new Object[argsNum];

        for (int i = 0; i < argsNum; ++i)
            args[i] = SqlListenerUtils.readObject(reader, false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteRequest.class, this);
    }
}

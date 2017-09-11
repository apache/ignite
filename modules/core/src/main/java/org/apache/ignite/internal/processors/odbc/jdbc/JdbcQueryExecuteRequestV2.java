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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC query execute request.
 */
public class JdbcQueryExecuteRequestV2 extends JdbcQueryExecuteRequest {
    /** Expected statement type. */
    JdbcStatementType stmtType;

    /**
     */
    JdbcQueryExecuteRequestV2() {
        super(QRY_EXEC_V2);
    }

    /**
     * @param stmtType Expected statement type.
     * @param schemaName Cache name.
     * @param pageSize Fetch size.
     * @param maxRows Max rows.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public JdbcQueryExecuteRequestV2(JdbcStatementType stmtType, String schemaName, int pageSize, int maxRows, String sqlQry,
        Object[] args) {
        super(QRY_EXEC_V2, schemaName, pageSize, maxRows, sqlQry, args);

        this.stmtType = stmtType;
    }

    /**
     * @return Boolean query flag {@code true} in case SELECT statement is expected,
     * {@code false} in case DML/DDL statement is expected, {@code null} any statement type is expected.
     */
    public JdbcStatementType expectedStatementType() {
        return stmtType;
    }


    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeByte((byte)stmtType.ordinal());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        stmtType = JdbcStatementType.values()[reader.readByte()];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteRequestV2.class, this);
    }
}

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

import java.sql.SQLException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Result carrying details needed to form proper {@link SQLException}.
 */
public class JdbcErrorResult extends JdbcResult {
    /**
     * Ignite specific error code.
     */
    private int code;

    /**
     * SQL state code.
     */
    private String sqlState;

    /**
     * Constructor.
     */
    public JdbcErrorResult() {
        super(JdbcResult.ERROR);
    }

    /**
     * Constructor.
     * @param code Ignite specific error code.
     * @param sqlState SQL state code.
     */
    public JdbcErrorResult(int code, String sqlState) {
        super(JdbcResult.ERROR);
        this.code = code;
        this.sqlState = sqlState;
    }

    /**
     * @return Ignite specific error code.
     * @see SQLException#getErrorCode() ()
     */
    public int code() {
        return code;
    }

    /**
     * @return SQL state code.
     * @see SQLException#getSQLState()
     */
    public String sqlState() {
        return sqlState;
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        code = reader.readInt();
        sqlState = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeInt(code);
        writer.writeString(sqlState);
    }
}

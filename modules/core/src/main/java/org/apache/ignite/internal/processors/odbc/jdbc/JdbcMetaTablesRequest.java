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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC tables metadata request.
 */
public class JdbcMetaTablesRequest extends JdbcRequest {
    /** Schema search pattern. */
    private String schemaName;

    /** Table search pattern. */
    private String tblName;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaTablesRequest() {
        super(META_TABLES);
    }

    /**
     * @param schemaName Schema search pattern.
     * @param tblName Table search pattern.
     */
    public JdbcMetaTablesRequest(String schemaName, String tblName) {
        super(META_TABLES);

        this.schemaName = schemaName;
        this.tblName = tblName;
    }

    /**
     * @return Schema search pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table search pattern.
     */
    public String tableName() {
        return tblName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);

        writer.writeString(schemaName);
        writer.writeString(tblName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);

        this.schemaName = reader.readString();
        this.tblName = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesRequest.class, this);
    }
}
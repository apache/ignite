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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC get columns metadata request.
 */
public class JdbcMetaColumnsRequest implements ClientMessage {
    /** Schema name pattern. */
    private String schemaName;

    /** Table name pattern. */
    private String tblName;

    /** Column name pattern. */
    private String colName;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaColumnsRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tblName    Table name.
     * @param colName    Column name.
     */
    public JdbcMetaColumnsRequest(String schemaName, String tblName, String colName) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;
    }

    /**
     * Gets schema name sql pattern.
     *
     * @return Schema name pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Gets table name sql pattern.
     *
     * @return Table name pattern.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Gets column name sql pattern.
     *
     * @return Column name pattern.
     */
    public String columnName() {
        return colName;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        packer.packString(schemaName);
        packer.packString(tblName);
        packer.packString(colName);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = unpacker.unpackString();
        tblName = unpacker.unpackString();
        colName = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcMetaColumnsRequest.class, this);
    }
}

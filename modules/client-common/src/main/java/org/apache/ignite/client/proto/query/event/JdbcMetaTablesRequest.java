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
 * JDBC tables metadata request.
 */
public class JdbcMetaTablesRequest implements ClientMessage {
    /** Schema search pattern. */
    private String schemaName;

    /** Table search pattern. */
    private String tblName;

    /** Table types. */
    private String[] tblTypes;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaTablesRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema search pattern.
     * @param tblName Table search pattern.
     * @param tblTypes Table types.
     */
    public JdbcMetaTablesRequest(String schemaName, String tblName, String[] tblTypes) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblTypes = tblTypes;
    }

    /**
     * Gets schema name pattern.
     *
     * @return Schema search pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Gets table name pattern.
     *
     * @return Table search pattern.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Gets allowed table types.
     *
     * @return Table types.
     */
    public String[] tableTypes() {
        return tblTypes;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        ClientMessageUtils.writeStringNullable(packer, schemaName);
        ClientMessageUtils.writeStringNullable(packer, tblName);

        if (tblTypes == null) {
            packer.packNil();

            return;
        }

        packer.packArrayHeader(tblTypes.length);

        for (String type : tblTypes)
            packer.packString(type);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = ClientMessageUtils.readStringNullable(unpacker);
        tblName = ClientMessageUtils.readStringNullable(unpacker);

        if (unpacker.tryUnpackNil())
            return;

        int size = unpacker.unpackArrayHeader();

        tblTypes = new String[size];

        for (int i = 0; i < size; i++)
            tblTypes[i] = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesRequest.class, this);
    }
}

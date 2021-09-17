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

import java.util.Objects;

import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC table metadata.
 */
public class JdbcTableMeta extends Response {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Table type. */
    private String tblType;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcTableMeta() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param tblType Table type.
     */
    public JdbcTableMeta(String schemaName, String tblName, String tblType) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblType = tblType;

        this.hasResults = true;
    }

    /**
     * Gets schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Gets table name.
     *
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Gets table type.
     *
     * @return Table type.
     */
    public String tableType() {
        return tblType;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults)
            return;

        packer.packString(schemaName);
        packer.packString(tblName);
        packer.packString(tblType);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults)
            return;

        schemaName = unpacker.unpackString();
        tblName = unpacker.unpackString();
        tblType = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcTableMeta meta = (JdbcTableMeta)o;

        return Objects.equals(schemaName, meta.schemaName)
            && Objects.equals(tblName, meta.tblName)
            && Objects.equals(tblType, meta.tblType);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + tblName.hashCode();
        result = 31 * result + tblType.hashCode();
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTableMeta.class, this);
    }
}

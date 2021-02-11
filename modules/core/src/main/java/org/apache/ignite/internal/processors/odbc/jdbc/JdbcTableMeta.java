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

import java.util.Objects;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.jdbc2.JdbcUtils.TYPE_TABLE;

/**
 * JDBC table metadata.
 */
public class JdbcTableMeta implements JdbcRawBinarylizable {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Table type. */
    private String tblType;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcTableMeta() {
        tblType = TYPE_TABLE;
    }

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param tblType Table type.
     */
    JdbcTableMeta(String schemaName, String tblName, String tblType) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblType = tblType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Table type.
     */
    public String tableType() {
        return tblType;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        writer.writeString(schemaName);
        writer.writeString(tblName);

        if (protoCtx.isTableTypesSupported())
            writer.writeString(tblType);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        schemaName = reader.readString();
        tblName = reader.readString();

        if (protoCtx.isTableTypesSupported())
            tblType = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcTableMeta meta = (JdbcTableMeta)o;

        return F.eq(schemaName, meta.schemaName) && F.eq(tblName, meta.tblName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schemaName, tblName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTableMeta.class, this);
    }
}

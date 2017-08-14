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
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC index metadata.
 */
public class JdbcIndexMeta implements JdbcRawBinarylizable {
    /** Index schema name. */
    private String schemaName;

    /** Index table name. */
    private String tblName;

    /** Index name. */
    private String idxName;

    /** Index type. */
    private QueryIndexType type;

    /** Index fields */
    private String[] fields;

    /** Index fields is ascending. */
    private boolean[] fieldsAsc;

    /**
     * Default constructor is used for binary serialization.
     */
    JdbcIndexMeta() {
        // No-op.
    }

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idx Index info.
     */
    JdbcIndexMeta(String schemaName, String tblName, GridQueryIndexDescriptor idx) {
        assert tblName != null;
        assert idx != null;
        assert idx.fields() != null;

        this.schemaName = schemaName;
        this.tblName = tblName;

        idxName = idx.name();
        type = idx.type();
        fields = idx.fields().toArray(new String[idx.fields().size()]);

        fieldsAsc = new boolean[fields.length];

        for (int i = 0; i < fields.length; ++i)
            fieldsAsc[i] = !idx.descending(fields[i]);
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
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Index type.
     */
    public QueryIndexType type() {
        return type;
    }

    /**
     * @return Index fields
     */
    public String[] fields() {
        return fields;
    }

    /**
     * @return Index fields is ascending.
     */
    public boolean[] fieldsAsc() {
        return fieldsAsc;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeString(schemaName);
        writer.writeString(tblName);
        writer.writeString(idxName);
        writer.writeByte((byte)type.ordinal());
        writer.writeStringArray(fields);
        writer.writeBooleanArray(fieldsAsc);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        schemaName = reader.readString();
        tblName = reader.readString();
        idxName = reader.readString();
        type = QueryIndexType.fromOrdinal(reader.readByte());
        fields = reader.readStringArray();
        fieldsAsc = reader.readBooleanArray();

        assert fields.length == fieldsAsc.length : "Fields info is broken: [fields.length=" + fields.length +
            ", fieldsAsc.length=" + fieldsAsc.length + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcIndexMeta meta = (JdbcIndexMeta)o;

        if (!F.eq(schemaName, meta.schemaName))
            return false;

        if (!F.eq(tblName, meta.tblName))
            return false;

        return idxName.equals(meta.idxName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;
        result = 31 * result + tblName.hashCode();
        result = 31 * result + idxName.hashCode();
        return result;
    }
}

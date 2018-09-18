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

import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMeta implements JdbcRawBinarylizable {
    /** Cache name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Column name. */
    private String colName;

    /** Data type. */
    private int dataType;

    /** Data type. */
    private String dataTypeName;

    /** Data type. */
    private String dataTypeClass;

    /**
     * Default constructor is used for serialization.
     */
    JdbcColumnMeta() {
        // No-op.
    }

    /**
     * @param info Field metadata.
     */
    JdbcColumnMeta(GridQueryFieldMetadata info) {
        this.schemaName = info.schemaName();
        this.tblName = info.typeName();
        this.colName = info.fieldName();

        dataType = JdbcThinUtils.type(info.fieldTypeName());
        dataTypeName = JdbcThinUtils.typeName(info.fieldTypeName());
        dataTypeClass = info.fieldTypeName();
    }

    /**
     * @param schemaName Schema.
     * @param tblName Table.
     * @param colName Column.
     * @param cls Type.
     */
    public JdbcColumnMeta(String schemaName, String tblName, String colName, Class<?> cls) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;

        String type = cls.getName();

        dataType = JdbcThinUtils.type(type);
        dataTypeName = JdbcThinUtils.typeName(type);
        dataTypeClass = type;
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
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /**
     * @return Column's data type.
     */
    public int dataType() {
        return dataType;
    }

    /**
     * @return Column's data type name.
     */
    public String dataTypeName() {
        return dataTypeName;
    }

    /**
     * @return Column's data type class.
     */
    public String dataTypeClass() {
        return dataTypeClass;
    }

    /**
     * @return Column's default value.
     */
    public String defaultValue() {
        return null;
    }

    /**
     * @return Column's precision.
     */
    public int precision() {
        return -1;
    }

    /**
     * @return Column's scale.
     */
    public int scale() {
        return -1;
    }

    /**
     * Return 'nullable' flag in compatibility mode (according with column name and column type).
     *
     * @return {@code true} in case the column allows null values. Otherwise returns {@code false}
     */
    public boolean isNullable() {
        return JdbcUtils.nullable(colName, dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) {
        writer.writeString(schemaName);
        writer.writeString(tblName);
        writer.writeString(colName);

        writer.writeInt(dataType);
        writer.writeString(dataTypeName);
        writer.writeString(dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) {
        schemaName = reader.readString();
        tblName = reader.readString();
        colName = reader.readString();

        dataType = reader.readInt();
        dataTypeName = reader.readString();
        dataTypeClass = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcColumnMeta meta = (JdbcColumnMeta)o;

        return F.eq(schemaName, meta.schemaName) && F.eq(tblName, meta.tblName) && F.eq(colName, meta.colName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;

        result = 31 * result + (tblName != null ? tblName.hashCode() : 0);
        result = 31 * result + colName.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcColumnMeta.class, this);
    }
}

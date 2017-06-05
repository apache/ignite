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
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * JDBC column metadata.
 */
public class JdbcColumnMeta implements JdbcRawBinarylizable {
    /** Cache name. */
    private String schema;

    /** Table name. */
    private String tbl;

    /** Column name. */
    private String col;

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
    }

    /**
     * @param info Field metadata.
     */
    JdbcColumnMeta(GridQueryFieldMetadata info) {
        this.schema = info.schemaName();
        this.tbl = info.typeName();
        this.col = info.fieldName();

        dataType = JdbcThinUtils.type(info.fieldTypeName());
        dataTypeName = JdbcThinUtils.typeName(info.fieldTypeName());
        dataTypeClass = info.fieldTypeName();
    }

    /**
     * @param schema Schema.
     * @param tableName Table.
     * @param col Column.
     * @param cls Type.
     */
    public JdbcColumnMeta(String schema, String tableName, String col, Class<?> cls) {
        this.schema = schema;
        this.tbl = tableName;
        this.col = col;

        String type = cls.getName();
        dataType = JdbcThinUtils.type(type);
        dataTypeName = JdbcThinUtils.typeName(type);
        dataTypeClass = type;
    }


        /**
         * @return Schema name.
         */
    public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tbl;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return col;
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

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) {
        writer.writeString(schema);
        writer.writeString(tbl);
        writer.writeString(col);

        writer.writeInt(dataType);
        writer.writeString(dataTypeName);
        writer.writeString(dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) {
        schema = reader.readString();
        tbl = reader.readString();
        col = reader.readString();

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

        if (schema != null ? !schema.equals(meta.schema) : meta.schema != null)
            return false;

        if (tbl != null ? !tbl.equals(meta.tbl) : meta.tbl!= null)
            return false;

        return col.equals(meta.col);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + (tbl != null ? tbl.hashCode() : 0);
        result = 31 * result + col.hashCode();
        return result;
    }
}

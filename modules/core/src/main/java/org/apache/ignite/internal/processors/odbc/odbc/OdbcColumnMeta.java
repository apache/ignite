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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL listener column metadata.
 */
public class OdbcColumnMeta {
    /** Cache name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Column name. */
    private final String columnName;

    /** Data type. */
    private final Class<?> dataType;

    /** Precision. */
    private final int precision;

    /** Scale. */
    private final int scale;

    /** Nullability. See {@link java.sql.ResultSetMetaData#isNullable(int)} for more info. */
    private final int nullability;

    /**
     * @param schemaName Cache name.
     * @param tableName Table name.
     * @param columnName Column name.
     * @param dataType Data type.
     * @param precision Precision.
     * @param scale Scale.
     * @param nullability Nullability.
     */
    public OdbcColumnMeta(String schemaName, String tableName, String columnName, Class<?> dataType,
        int precision, int scale, int nullability) {
        this.schemaName = OdbcUtils.addQuotationMarksIfNeeded(schemaName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
        this.nullability = nullability;
    }

    /**
     * Constructor for result set column.
     * @param info Field metadata.
     */
    public OdbcColumnMeta(GridQueryFieldMetadata info) {
        schemaName = OdbcUtils.addQuotationMarksIfNeeded(info.schemaName());
        tableName = info.typeName();
        columnName = info.fieldName();
        precision = info.precision();
        scale = info.scale();
        nullability = info.nullability();

        Class<?> type;

        try {
            type = Class.forName(info.fieldTypeName());
        }
        catch (Exception ignored) {
            type = Object.class;
        }

        this.dataType = type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hash = schemaName.hashCode();

        hash = 31 * hash + tableName.hashCode();
        hash = 31 * hash + columnName.hashCode();
        hash = 31 * hash + dataType.hashCode();
        hash = 31 * hash + Integer.hashCode(precision);
        hash = 31 * hash + Integer.hashCode(scale);

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o instanceof OdbcColumnMeta) {
            OdbcColumnMeta other = (OdbcColumnMeta)o;

            return this == other || schemaName.equals(other.schemaName) && tableName.equals(other.tableName) &&
                columnName.equals(other.columnName) && dataType.equals(other.dataType) &&
                precision == other.precision && scale == other.scale;
        }

        return false;
    }

    /**
     * Write in a binary format.
     *
     * @param writer Binary writer.
     * @param ver Client version.
     */
    public void write(BinaryRawWriter writer, ClientListenerProtocolVersion ver) {
        writer.writeString(schemaName);
        writer.writeString(tableName);
        writer.writeString(columnName);

        byte typeId = getTypeId(dataType);

        writer.writeByte(typeId);

        if (ver.compareTo(OdbcConnectionContext.VER_2_7_0) >= 0) {
            writer.writeInt(precision);
            writer.writeInt(scale);
        }

        if (ver.compareTo(OdbcConnectionContext.VER_2_8_0) >= 0) {
            writer.writeByte((byte)nullability);
        }
    }

    /**
     * Get ODBC type ID for the type.
     * @param dataType Data type class.
     * @return Type ID.
     */
    private static byte getTypeId(Class<?> dataType) {
        if (dataType.equals(java.sql.Date.class))
            return GridBinaryMarshaller.DATE;

        return BinaryUtils.typeByClass(dataType);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcColumnMeta.class, this);
    }
}

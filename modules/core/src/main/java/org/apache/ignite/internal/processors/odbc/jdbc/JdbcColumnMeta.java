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

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * SQL listener column metadata.
 */
public class JdbcColumnMeta implements RawBinarylizable {
    /** Cache name. */
    private String schemaName;

    /** Table name. */
    private String tableName;

    /** Column name. */
    private String columnName;

    /** Data type. */
    private int dataType;

    /** Data type. */
    private String dataTypeName;

    /** Data type. */
    private String dataTypeClass;

    /**
     * Default constructor is used for serialization.
     */
    public JdbcColumnMeta() {
    }

    /**
     * @param info Field metadata.
     */
    public JdbcColumnMeta(GridQueryFieldMetadata info) {
        this.schemaName = info.schemaName();
        this.tableName = info.typeName();
        this.columnName = info.fieldName();

        Class<?> cls;

        try {
            cls = Class.forName(info.fieldTypeName());
        }
        catch (Exception ignored) {
            cls = Object.class;
        }

        dataType = type(cls);
        dataTypeName = typeName(cls);
        dataTypeClass = cls.getName();
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
        return tableName;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return columnName;
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
        writer.writeString(schemaName);
        writer.writeString(tableName);
        writer.writeString(columnName);

        writer.writeInt(dataType);
        writer.writeString(dataTypeName);
        writer.writeString(dataTypeClass);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) {
        schemaName = reader.readString();
        tableName = reader.readString();
        columnName = reader.readString();

        dataType = reader.readInt();
        dataTypeName = reader.readString();
        dataTypeClass = reader.readString();
    }

    /**
     * Converts Java class name to type from {@link Types}.
     *
     * @param cls Java class name.
     * @return Type from {@link Types}.
     */
    @SuppressWarnings("IfMayBeConditional")
    public static int type(Class<?> cls) {
        if (Boolean.class.equals(cls) || boolean.class.equals(cls))
            return BOOLEAN;
        else if (Byte.class.equals(cls) || byte.class.equals(cls))
            return TINYINT;
        else if (Short.class.equals(cls) || short.class.equals(cls))
            return SMALLINT;
        else if (Integer.class.equals(cls) || int.class.equals(cls))
            return INTEGER;
        else if (Long.class.equals(cls) || long.class.equals(cls))
            return BIGINT;
        else if (Float.class.equals(cls) || float.class.equals(cls))
            return FLOAT;
        else if (Double.class.equals(cls) || double.class.equals(cls))
            return DOUBLE;
        else if (String.class.equals(cls))
            return VARCHAR;
        else if (byte[].class.equals(cls))
            return BINARY;
        else if (Time.class.equals(cls))
            return TIME;
        else if (Timestamp.class.equals(cls))
            return TIMESTAMP;
        else if (Date.class.equals(cls))
            return DATE;
        else
            return OTHER;
    }

    /**
     * Converts Java class name to SQL type name.
     *
     * @param cls Java class name.
     * @return SQL type name.
     */
    @SuppressWarnings("IfMayBeConditional")
    public static String typeName(Class<?> cls) {
        if (Boolean.class.equals(cls) || boolean.class.equals(cls))
            return "BOOLEAN";
        else if (Byte.class.equals(cls) || byte.class.equals(cls))
            return "TINYINT";
        else if (Short.class.equals(cls) || short.class.equals(cls))
            return "SMALLINT";
        else if (Integer.class.equals(cls) || int.class.equals(cls))
            return "INTEGER";
        else if (Long.class.equals(cls) || long.class.equals(cls))
            return "BIGINT";
        else if (Float.class.equals(cls) || float.class.equals(cls))
            return "FLOAT";
        else if (Double.class.equals(cls) || double.class.equals(cls))
            return "DOUBLE";
        else if (String.class.equals(cls))
            return "VARCHAR";
        else if (byte[].class.equals(cls))
            return "BINARY";
        else if (Time.class.equals(cls))
            return "TIME";
        else if (Timestamp.class.equals(cls))
            return "TIMESTAMP";
        else if (Date.class.equals(cls))
            return "DATE";
        else
            return "OTHER";
    }

}

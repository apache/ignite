/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;

import org.h2.value.Transfer;
import org.h2.value.TypeInfo;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {

    /**
     * The column alias.
     */
    final String alias;

    /**
     * The schema name or null.
     */
    final String schemaName;

    /**
     * The table name or null.
     */
    final String tableName;

    /**
     * The column name or null.
     */
    final String columnName;

    /**
     * The column type.
     */
    final TypeInfo columnType;

    /**
     * True if this is an autoincrement column.
     */
    final boolean autoIncrement;

    /**
     * True if this column is nullable.
     */
    final int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
    ResultColumn(Transfer in) throws IOException {
        alias = in.readString();
        schemaName = in.readString();
        tableName = in.readString();
        columnName = in.readString();
        int valueType = in.readInt();
        long precision = in.readLong();
        int scale = in.readInt();
        int displaySize = in.readInt();
        columnType = new TypeInfo(valueType, precision, scale, displaySize, null);
        autoIncrement = in.readBoolean();
        nullable = in.readInt();
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
    public static void writeColumn(Transfer out, ResultInterface result, int i)
            throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        TypeInfo type = result.getColumnType(i);
        out.writeInt(type.getValueType());
        out.writeLong(type.getPrecision());
        out.writeInt(type.getScale());
        out.writeInt(type.getDisplaySize());
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }

}

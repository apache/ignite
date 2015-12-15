package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

import java.io.IOException;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.UNREGISTERED_TYPE_ID;

/**
 * ODBC column-related metadata.
 */
public class GridOdbcColumnMeta {
    /** Cache name. */
    private String schemaName;

    /** Table name. */
    private String tableName;

    /** Column name. */
    private String columnName;

    /** Data type. */
    private Class<?> dataType;

    /**
     * Add quotation marks at the beginning and end of the string.
     * @param str Input string.
     * @return String surrounded with quotation marks.
     */
    private String AddQuotationMarksIfNeeded(String str) {
        if (!str.startsWith("\"") && !str.isEmpty())
            return "\"" + str + "\"";

        return str;
    }

    /**
     * @param schemaName Cache name.
     * @param tableName Table name.
     * @param columnName Column name.
     * @param dataType Data type.
     */
    public GridOdbcColumnMeta(String schemaName, String tableName, String columnName, Class<?> dataType) {
        this.schemaName = AddQuotationMarksIfNeeded(schemaName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
    }

    /**
     * @param info Field metadata.
     */
    public GridOdbcColumnMeta(GridQueryFieldMetadata info) {
        this.schemaName = AddQuotationMarksIfNeeded(info.schemaName());
        this.tableName = info.typeName();
        this.columnName = info.fieldName();

        try {
            this.dataType = Class.forName(info.fieldTypeName());
        }
        catch (Exception ignoreed) {
            this.dataType = Object.class;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GridOdbcColumnMeta))
            return false;

        GridOdbcColumnMeta another = (GridOdbcColumnMeta)o;

        return schemaName.equals(another.schemaName) &&
               tableName.equals(another.tableName)   &&
               columnName.equals(another.columnName) &&
               dataType.equals(another.dataType);
    }

    /**
     * Write in a binary format.
     * @param writer Binary writer.
     * @param ctx Portable context.
     * @throws IOException
     */
    public void writeBinary(BinaryRawWriterEx writer, BinaryContext ctx) throws IOException {
        writer.writeString(schemaName);
        writer.writeString(tableName);
        writer.writeString(columnName);
        writer.writeString(dataType.getName());

        byte typeId;

        BinaryClassDescriptor desc = ctx.descriptorForClass(dataType, false);

        if (desc == null)
            throw new IOException("Object is not portable: [class=" + dataType + ']');

        if (desc.registered())
            typeId = (byte)desc.typeId();
        else
            typeId = (byte)UNREGISTERED_TYPE_ID;

        writer.writeByte(typeId);
    }
}

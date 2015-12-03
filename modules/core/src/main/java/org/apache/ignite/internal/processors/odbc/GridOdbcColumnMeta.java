package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.portable.BinaryRawWriterEx;
import org.apache.ignite.internal.portable.PortableClassDescriptor;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

import java.io.IOException;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLASS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;

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
     * @param schemaName Cache name.
     * @param tableName Table name.
     * @param columnName Column name.
     * @param dataType Data type.
     */
    public GridOdbcColumnMeta(String schemaName, String tableName, String columnName, Class<?> dataType) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
    }

    /**
     * @param info Field metadata.
     */
    public GridOdbcColumnMeta(GridQueryFieldMetadata info) {
        this.schemaName = info.schemaName();
        this.tableName = info.typeName();
        this.columnName = info.fieldName();

        try {
            this.dataType = Class.forName(info.fieldTypeName());
        }
        catch (Exception ignoreed) {
            this.dataType = Object.class;
        }
    }


    /**
     * @return Cache name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @return Data type.
     */
    public Class<?> getDataType() {
        return dataType;
    }

    /**
     * Write in a binary format.
     * @param writer Binary writer.
     * @param ctx Binary context.
     * @throws IOException
     */
    public void writeBinary(BinaryRawWriterEx writer, PortableContext ctx) throws IOException {
        writer.writeString(schemaName);
        writer.writeString(tableName);
        writer.writeString(columnName);
        writer.writeString(dataType.getName());

        PortableClassDescriptor desc = ctx.descriptorForClass(dataType);

        if (desc == null)
            throw new IOException("Object is not portable: [class=" + dataType + ']');

        if (desc.registered())
            writer.writeByte((byte) desc.typeId());
        else
            writer.writeByte((byte) UNREGISTERED_TYPE_ID);
    }
}

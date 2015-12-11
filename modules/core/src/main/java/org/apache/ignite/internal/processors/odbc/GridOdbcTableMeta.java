package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;

import java.io.IOException;

/**
 * ODBC table-related metadata.
 */
public class GridOdbcTableMeta {
    /** Catalog name. */
    private String catalog;

    /** Schema name. */
    private String schema;

    /** Table name. */
    private String table;

    /** Table type. */
    private String tableType;

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
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param tableType Table type.
     */
    public GridOdbcTableMeta(String catalog, String schema, String table, String tableType) {
        this.catalog = catalog;
        this.schema = AddQuotationMarksIfNeeded(schema);
        this.table = table;
        this.tableType = tableType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GridOdbcTableMeta))
            return false;

        GridOdbcTableMeta another = (GridOdbcTableMeta)o;

        return catalog.equals(another.catalog) &&
               schema.equals(another.schema) &&
               table.equals(another.table) &&
               tableType.equals(another.tableType);
    }

    /**
     * Write in a binary format.
     * @param writer Binary writer.
     * @throws IOException
     */
    public void writeBinary(BinaryRawWriterEx writer) throws IOException {
        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(table);
        writer.writeString(tableType);
    }
}

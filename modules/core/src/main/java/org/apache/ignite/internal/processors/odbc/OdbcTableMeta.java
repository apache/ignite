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
package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;

import java.io.IOException;

/**
 * ODBC table-related metadata.
 */
public class OdbcTableMeta {
    /** Catalog name. */
    private final String catalog;

    /** Schema name. */
    private final String schema;

    /** Table name. */
    private final String table;

    /** Table type. */
    private final String tableType;

    /**
     * Add quotation marks at the beginning and end of the string.
     *
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
    public OdbcTableMeta(String catalog, String schema, String table, String tableType) {
        this.catalog = catalog;
        this.schema = AddQuotationMarksIfNeeded(schema);
        this.table = table;
        this.tableType = tableType;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o)
    {
        if (!(o instanceof OdbcTableMeta))
            return false;

        OdbcTableMeta another = (OdbcTableMeta)o;

        return catalog.equals(another.catalog) &&
               schema.equals(another.schema) &&
               table.equals(another.table) &&
               tableType.equals(another.tableType);
    }

    /**
     * Write in a binary format.
     *
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

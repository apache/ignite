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

import java.util.Objects;

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
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param tableType Table type.
     */
    public OdbcTableMeta(String catalog, String schema, String table, String tableType) {
        this.catalog = catalog;
        this.schema = OdbcUtils.addQuotationMarksIfNeeded(schema);
        this.table = table;
        this.tableType = tableType;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hash = Objects.hashCode(catalog);

        hash = 31 * hash + Objects.hashCode(schema);
        hash = 31 * hash + Objects.hashCode(table);
        hash = 31 * hash + Objects.hashCode(tableType);

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o instanceof OdbcTableMeta) {
            OdbcTableMeta other = (OdbcTableMeta) o;

            return this == other ||
                    Objects.equals(catalog, other.catalog) && Objects.equals(schema, other.schema) &&
                    Objects.equals(table, other.table) && Objects.equals(tableType, other.tableType);
        }

        return false;
    }

    /**
     * Write in a binary format.
     *
     * @param writer Binary writer.
     */
    public void writeBinary(BinaryRawWriterEx writer) {
        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(table);
        writer.writeString(tableType);
    }
}

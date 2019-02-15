/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.odbc.odbc;

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

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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query get tables meta request.
 */
public class JdbcMetaTablesRequest extends JdbcRequest {
    /** Catalog search pattern. */
    private String catalog;

    /** Schema search pattern. */
    private String schema;

    /** Table search pattern. */
    private String table;

    /** Table types. */
    private String[] tableTypes;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaTablesRequest() {
        super(META_TABLES);
    }

    /**
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param tableTypes Table types.
     */
    public JdbcMetaTablesRequest(String catalog, String schema, String table, String[] tableTypes) {
        super(META_TABLES);

        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.tableTypes = tableTypes;
    }

    /**
     * @return catalog search pattern.
     */
    public String catalog() {
        return catalog;
    }

    /**
     * @return Schema search pattern.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table search pattern.
     */
    public String table() {
        return table;
    }

    /**
     * @return Table type search pattern.
     */
    public String[] tableTypes() {
        return tableTypes;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(table);
        writer.writeStringArray(tableTypes);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        this.catalog = reader.readString();
        this.schema = reader.readString();
        this.table = reader.readString();
        this.tableTypes = reader.readStringArray();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesRequest.class, this);
    }
}
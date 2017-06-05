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

/**
 * JDBC table metadata.
 */
public class JdbcTableMeta implements JdbcRawBinarylizable {
    /** Catalog name. */
    private String catalog;

    /** Schema name. */
    private String schema;

    /** Table name. */
    private String tbl;

    /** Table type. */
    private String tblType;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcTableMeta() {
    }

    /**
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param tbl Table name.
     * @param tblType Table type.
     */
    JdbcTableMeta(String catalog, String schema, String tbl, String tblType) {
        this.catalog = catalog;
        this.schema = schema;
        this.tbl = tbl;
        this.tblType = tblType;
    }

    /**
     * @return Table's catalog.
     */
    public String catalog() {
        return catalog;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String table() {
        return tbl;
    }

    /**
     * @return Table type.
     */
    public String tableType() {
        return tblType;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(tbl);
        writer.writeString(tblType);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        catalog = reader.readString();
        schema = reader.readString();
        tbl = reader.readString();
        tblType = reader.readString();
    }
}

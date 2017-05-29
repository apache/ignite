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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query get tables meta request.
 */
public class OdbcQueryGetTablesMetaRequest extends SqlListenerRequest {
    /** Catalog search pattern. */
    private String catalog;

    /** Schema search pattern. */
    private String schema;

    /** Table search pattern. */
    private String table;

    /** Table type search pattern. */
    private String tableType;

    /**
     *
     */
    public OdbcQueryGetTablesMetaRequest() {
        super(META_TBLS);
    }

    /**
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param tableType Table type search pattern.
     */
    public OdbcQueryGetTablesMetaRequest(String catalog, String schema, String table, String tableType) {
        super(META_TBLS);

        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.tableType = tableType;
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
    public String tableType() {
        return tableType;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        SqlListenerAbstractObjectWriter objWriter) throws BinaryObjectException {
        super.writeBinary(writer, objWriter);

        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(table);
        writer.writeString(tableType);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        SqlListenerAbstractObjectReader objReader) throws BinaryObjectException {
        super.readBinary(reader, objReader);

        catalog = reader.readString();
        schema = reader.readString();
        table = reader.readString();
        tableType = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetTablesMetaRequest.class, this);
    }
}
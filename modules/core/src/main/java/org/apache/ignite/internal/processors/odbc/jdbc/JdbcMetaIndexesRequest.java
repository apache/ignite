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
import org.jetbrains.annotations.Nullable;

/**
 * JDBC indexes metadata request.
 */
public class JdbcMetaIndexesRequest extends JdbcRequest {
    /** Cache name. */
    private String catalog;

    /** Cache name. */
    private String schema;

    /** Table name. */
    private String tblName;

    /** When true, return only indices for unique values. */
    private boolean unique;

    /** When true, result is allowed to reflect approximate or out of data values. */
    private boolean approximate;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaIndexesRequest() {
        super(META_INDEXES);
    }

    /**
     * @param catalog Catalog name.
     * @param schema Cache name.
     * @param tblName Table name.
     * @param unique {@code true} when only indices for unique values are requested.
     * @param approximate {@code true} when approximate or out of data values indexes are allowed in results.
     */
    public JdbcMetaIndexesRequest(String catalog, String schema, String tblName, boolean unique, boolean approximate) {
        super(META_INDEXES);

        this.catalog = catalog;
        this.schema = schema;
        this.tblName = tblName;
        this.unique = unique;
        this.approximate = approximate;
    }

    /**
     * @return Catalog name.
     */
    @Nullable public String catalog() {
        return catalog;
    }

    /**
     * @return Schema name.
     */
    @Nullable public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return {@code true} when only indices for unique values are requested.
     */
    public boolean unique() {
        return unique;
    }

    /**
     * @return {@code true} when approximate or out of data values indexes are allowed in results.
     */
    public boolean approximate() {
        return approximate;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(catalog);
        writer.writeString(schema);
        writer.writeString(tblName);
        writer.writeBoolean(unique);
        writer.writeBoolean(approximate);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        catalog = reader.readString();
        schema = reader.readString();
        tblName = reader.readString();
        unique = reader.readBoolean();
        approximate = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaIndexesRequest.class, this);
    }
}
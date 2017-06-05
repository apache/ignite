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
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;

/**
 * JDBC index metadata.
 */
public class JdbcPrimaryKeyMeta implements JdbcRawBinarylizable {
    /** Cache name. */
    private String schema;

    /** Table name. */
    private String tbl;

    /** Primary key name. */
    private String name;

    /** Index fields */
    private String[] fields;

    /**
     * Default constructor is used for binary serialization.
     */
    JdbcPrimaryKeyMeta() {
        // No-op.
    }

    /**
     * @param idx Index info.
     */
    JdbcPrimaryKeyMeta(GridQueryFieldMetadata idx) {

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
    public String tableName() {
        return tbl;
    }

    /**
     * @return Primary key name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Index fields
     */
    public String[] fields() {
        return fields;
    }


    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeString(schema);
        writer.writeString(tbl);
        writer.writeString(name);
        writer.writeStringArray(fields);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        schema = reader.readString();
        tbl = reader.readString();
        name = reader.readString();
        fields = reader.readStringArray();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcPrimaryKeyMeta meta = (JdbcPrimaryKeyMeta)o;

        return name.equals(meta.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }
}

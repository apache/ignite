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
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;

/**
 * JDBC index metadata.
 */
public class JdbcIndexMeta implements JdbcRawBinarylizable {
    /** Index name. */
    private String name;

    /** Index type. */
    private QueryIndexType type;

    /** Inline size. */
    private int inlineSize;

    /** Index fields */
    private String[] fields;

    /** Index fields is ascending. */
    private boolean[] fieldsAsc;

    /**
     * Default constructor is used for binary serialization.
     */
    JdbcIndexMeta() {
        // No-op.
    }

    /**
     * @param idx Index info.
     */
    JdbcIndexMeta(GridQueryIndexDescriptor idx) {
        assert idx != null;
        assert idx.fields() != null;

        name = idx.name();
        type = idx.type();
        inlineSize = idx.inlineSize();
        fields = idx.fields().toArray(new String[idx.fields().size()]);

        fieldsAsc = new boolean[fields.length];

        for (int i = 0; i < fields.length; ++i)
            fieldsAsc[i] = !idx.descending(fields[i]);
    }

    /**
     * @return Index name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Index type.
     */
    public QueryIndexType type() {
        return type;
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Index fields
     */
    public String[] fields() {
        return fields;
    }

    /**
     * @return Index fields is ascending.
     */
    public boolean[] fieldsAsc() {
        return fieldsAsc;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeString(name);
        writer.writeByte((byte)type.ordinal());
        writer.writeInt(inlineSize);
        writer.writeStringArray(fields);
        writer.writeBooleanArray(fieldsAsc);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        name = reader.readString();
        type = QueryIndexType.fromOrdinal(reader.readByte());
        inlineSize = reader.readInt();
        fields = reader.readStringArray();
        fieldsAsc = reader.readBooleanArray();

        assert fields.length == fieldsAsc.length : "Fields info is broken: [fields.length=" + fields.length +
            ", fieldsAsc.length=" + fieldsAsc.length + ']';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcIndexMeta meta = (JdbcIndexMeta)o;

        return name.equals(meta.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }
}

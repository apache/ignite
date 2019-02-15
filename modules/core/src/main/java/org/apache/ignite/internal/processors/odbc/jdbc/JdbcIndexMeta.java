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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC index metadata.
 */
public class JdbcIndexMeta implements JdbcRawBinarylizable {
    /** Index schema name. */
    private String schemaName;

    /** Index table name. */
    private String tblName;

    /** Index name. */
    private String idxName;

    /** Index type. */
    private QueryIndexType type;

    /** Index fields */
    private List<String> fields;

    /** Index fields is ascending. */
    private List<Boolean> fieldsAsc;

    /**
     * Default constructor is used for binary serialization.
     */
    JdbcIndexMeta() {
        // No-op.
    }

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idx Index info.
     */
    JdbcIndexMeta(String schemaName, String tblName, GridQueryIndexDescriptor idx) {
        assert tblName != null;
        assert idx != null;
        assert idx.fields() != null;

        this.schemaName = schemaName;
        this.tblName = tblName;

        idxName = idx.name();
        type = idx.type();
        fields = new ArrayList(idx.fields());

        fieldsAsc = new ArrayList<>(fields.size());

        for (int i = 0; i < fields.size(); ++i)
            fieldsAsc.add(!idx.descending(fields.get(i)));
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Index type.
     */
    public QueryIndexType type() {
        return type;
    }

    /**
     * @return Index fields
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * @return Index fields is ascending.
     */
    public List<Boolean> fieldsAsc() {
        return fieldsAsc;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        writer.writeString(schemaName);
        writer.writeString(tblName);
        writer.writeString(idxName);
        writer.writeByte((byte)type.ordinal());

        JdbcUtils.writeStringCollection(writer, fields);

        if (fieldsAsc == null)
            writer.writeInt(0);
        else {
            writer.writeInt(fieldsAsc.size());

            for (Boolean b : fieldsAsc)
                writer.writeBoolean(b.booleanValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        schemaName = reader.readString();
        tblName = reader.readString();
        idxName = reader.readString();
        type = QueryIndexType.fromOrdinal(reader.readByte());

        fields = JdbcUtils.readStringList(reader);

        int size = reader.readInt();

        if (size > 0) {
            fieldsAsc = new ArrayList<>(size);

            for (int i = 0; i < size; ++i)
                fieldsAsc .add(reader.readBoolean());
        }
        else
            fieldsAsc = Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        JdbcIndexMeta meta = (JdbcIndexMeta)o;

        return F.eq(schemaName, meta.schemaName) && F.eq(tblName, meta.tblName) && F.eq(idxName, meta.idxName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName != null ? schemaName.hashCode() : 0;

        result = 31 * result + tblName.hashCode();
        result = 31 * result + idxName.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcIndexMeta.class, this);
    }
}

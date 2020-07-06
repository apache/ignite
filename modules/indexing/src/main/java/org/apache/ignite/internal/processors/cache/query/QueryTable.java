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

package org.apache.ignite.internal.processors.cache.query;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Query table descriptor.
 */
public class QueryTable implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema. */
    private String schema;

    /** Table. */
    private String tbl;

    /**
     * Defalt constructor.
     */
    public QueryTable() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param tbl Table.
     */
    public QueryTable(String schema, String tbl) {
        this.schema = schema;
        this.tbl = tbl;
    }

    /**
     * @return Schema.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table.
     */
    public String table() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString("schema", schema))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("tbl", tbl))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                schema = reader.readString("schema");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                tbl = reader.readString("tbl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(QueryTable.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -54;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (schema != null ? schema.hashCode() : 0) + (tbl != null ? tbl.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj instanceof QueryTable) {
            QueryTable other = (QueryTable)obj;

            return F.eq(tbl, other.tbl) && F.eq(schema, other.schema);
        }

        return super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTable.class, this);
    }
}

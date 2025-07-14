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

package org.apache.ignite.internal.processors.query.h2;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Query table descriptor.
 */
public class QueryTable implements Message {
    /** Schema. */
    private String schema;

    /** Table. */
    private String tbl;

    /**
     * Empty constructor required by {@link GridH2ValueMessageFactory}.
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
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeString(schema))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString(tbl))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                schema = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                tbl = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -54;
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

            return Objects.equals(tbl, other.tbl) && Objects.equals(schema, other.schema);
        }

        return super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTable.class, this);
    }
}

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
import java.util.Set;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
// TODO VO: Do we really need to pass "cols" at the moment? I would remove it.

// TODO VO: Consider encapsulating map here to minimize message size?
//class Info {
//    String[] tblNames;
//    boolean[] valUsed;
//}

public class GridSqlUsedColumnInfo implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Columns to extract from full row. */
    @GridToStringInclude
    private int[] cols;

    /** Flag indicates that key's fields are used in query. */
    @GridToStringInclude
    private boolean keyUsed;

    /** Flag indicates that value's fields are used in query. */
    @GridToStringInclude
    private boolean valUsed;

    /**
     * Constructor.
     */
    public GridSqlUsedColumnInfo() {
        // No-op.
    }

    /**
     * @param cols Columns to extract from full row.
     * @param keyUsed Flag indicates that key's fields are used in query.
     * @param valUsed Flag indicates that value's fields are used in query.
     */
    public GridSqlUsedColumnInfo(Set<Integer> cols, boolean keyUsed, boolean valUsed) {
        assert keyUsed || valUsed;

        this.cols = F.isEmpty(cols) ? null : cols.stream().mapToInt(Number::intValue).toArray();
        this.keyUsed = keyUsed;
        this.valUsed = valUsed;
    }

    /**
     * @return Columns IDs to extract from full row.
     */
    public int[] columns() {
        return cols;
    }

    /**
     * @return Flag indicates that key's fields are used in query.
     */
    public boolean isKeyUsed() {
        return keyUsed;
    }

    /**
     * @param colInfo Extracted columns info.
     * @return row data mode.
     */
    public static CacheDataRowAdapter.RowData asRowData(GridSqlUsedColumnInfo colInfo) {
        // Don't use optimisation RowData.NO_KEY when keyUsed is false
        // because the last row on page is used to lookup the next rown on the next page
        // and the key is used to compare. The optimization have to change the ForwardCursor logic.
        if (colInfo != null && !colInfo.isValueUsed())
            return CacheDataRowAdapter.RowData.KEY_ONLY;
        else
            return CacheDataRowAdapter.RowData.FULL;
    }

    /**
     * @return Flag indicates that value's fields are used in query.
     */
    public boolean isValueUsed() {
        return valUsed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSqlUsedColumnInfo.class, this);
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
                if (!writer.writeIntArray("cols", cols))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("keyUsed", keyUsed))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("valUsed", valUsed))
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
                cols = reader.readIntArray("cols");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                keyUsed = reader.readBoolean("keyUsed");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                valUsed = reader.readBoolean("valUsed");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridSqlUsedColumnInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 172;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}

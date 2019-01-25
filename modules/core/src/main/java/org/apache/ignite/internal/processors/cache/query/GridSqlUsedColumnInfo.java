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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridSqlUsedColumnInfo implements Message {
    /** Columns to extract from full row. */
    @GridToStringInclude
    private int[] cols;

    /** Extract key. */
    @GridToStringInclude
    private boolean extractKey;

    /** Extract value. */
    @GridToStringInclude
    private boolean extractVal;

    /**
     * Constructor.
     */
    public GridSqlUsedColumnInfo() {
        // No-op.
    }

    /**
     * @param cols Columns to extract from full row.
     * @param extractKey flag indicates that key's fields are used in query.
     * @param extractVal flag indicates that value's fields are used in query.
     */
    public GridSqlUsedColumnInfo(int[] cols, boolean extractKey, boolean extractVal) {
        this.cols = cols;
        this.extractKey = extractKey;
        this.extractVal = extractVal;
    }

    /**
     * @return Columns IDs to extract from full row.
     */
    public int[] columns() {
        return cols;
    }

    /**
     * @return {@code true} if key's fields are used in query.
     */
    public boolean isExtractKey() {
        return extractKey;
    }

    /**
     * @return {@code true} if value's fields are used in query.
     */
    public boolean isExtractValue() {
        return extractVal;
    }

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
                if (!writer.writeBoolean("extractKey", extractKey))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("extractVal", extractVal))
                    return false;

                writer.incrementState();

        }

        return true;
    }

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
                extractKey = reader.readBoolean("extractKey");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                extractVal = reader.readBoolean("extractVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridSqlUsedColumnInfo.class);
    }

    @Override public short directType() {
        return 172;
    }

    @Override public byte fieldsCount() {
        return 3;
    }

    @Override public void onAckReceived() {

    }
}

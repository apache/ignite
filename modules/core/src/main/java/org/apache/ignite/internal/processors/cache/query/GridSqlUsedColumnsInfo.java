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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridSqlUsedColumnsInfo implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Columns to extract from full row. */
    @GridToStringInclude
    private String[] aliases;

    /** Flag indicates that value's fields are used in query. */
    @GridToStringInclude
    private boolean[] valUsed;

    /**
     * Constructor.
     */
    public GridSqlUsedColumnsInfo() {
        // No-op.
    }

    /**
     * @param mapValUsed Map of flags indicates that value's fields are used in query by table alias (Table alias -> flag).
     */
    public GridSqlUsedColumnsInfo(Map<String, Boolean> mapValUsed) {
        aliases = new String[mapValUsed.size()];
        valUsed = new boolean[mapValUsed.size()];

        int i = 0;
        for (Map.Entry<String, Boolean> e : mapValUsed.entrySet()) {
            aliases[i] = e.getKey();
            valUsed[i] = e.getValue();
            ++i;
        }
    }

    /**
     *
     * @return The map table aliases to value used flag for the table.
     */
    public Map<String, Boolean> createValueUsedMap() {
        assert aliases.length == valUsed.length : "Inconsistent arrays lenght: " +
            "[aliases=" + S.arrayToString(aliases) + ", valUsed=" + S.arrayToString(valUsed);

        Map<String, Boolean> mapValUsed = new HashMap<>(aliases.length);

        for (int i = 0; i < aliases.length; ++i)
            mapValUsed.put(aliases[i], valUsed[i]);

        return mapValUsed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSqlUsedColumnsInfo.class, this);
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
                if (!writer.writeObjectArray("aliases", aliases, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBooleanArray("valUsed", valUsed))
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
                aliases = reader.readObjectArray("aliases", MessageCollectionItemType.STRING, String.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                valUsed = reader.readBooleanArray("valUsed");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridSqlUsedColumnsInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 172;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}

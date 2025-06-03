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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.stat.StatisticsType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Request for statistics.
 */
public class StatisticsRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 187;

    /** Gathering id. */
    private UUID reqId;

    /** Key to supply statistics by. */
    private StatisticsKeyMessage key;

    /** Type of required statistcs. */
    private StatisticsType type;

    /** For local statistics request - column config versions map: name to version. */
    @GridDirectMap(keyType = String.class, valueType = Long.class)
    private Map<String, Long> versions;

    /** For local statistics request - version to gather statistics by. */
    private AffinityTopologyVersion topVer;

    /**
     * Constructor.
     */
    public StatisticsRequest() {
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param key Statistics key to get statistics by.
     * @param type Required statistics type.
     * @param topVer Topology version to get statistics by.
     * @param versions Map of statistics version to column name to ensure actual statistics aquired.
     */
    public StatisticsRequest(
        UUID reqId,
        StatisticsKeyMessage key,
        StatisticsType type,
        AffinityTopologyVersion topVer,
        Map<String, Long> versions
    ) {
        this.reqId = reqId;
        this.key = key;
        this.type = type;
        this.topVer = topVer;
        this.versions = versions;
    }

    /** Request id. */
    public UUID reqId() {
        return reqId;
    }

    /**
     * @return Required statistics type.
     */
    public StatisticsType type() {
        return type;
    }

    /**
     * @return Key for required statitics.
     */
    public StatisticsKeyMessage key() {
        return key;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /**
     * @return Column name to version map.
     */
    public Map<String, Long> versions() {
        return versions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsRequest.class, this);
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
                if (!writer.writeMessage(key))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid(reqId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeAffinityTopologyVersion(topVer))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByte(type != null ? (byte)type.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(versions, MessageCollectionItemType.STRING, MessageCollectionItemType.LONG))
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
                key = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                reqId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                topVer = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                byte typeOrd;

                typeOrd = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                type = StatisticsType.fromOrdinal(typeOrd);

                reader.incrementState();

            case 4:
                versions = reader.readMap(MessageCollectionItemType.STRING, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }
}

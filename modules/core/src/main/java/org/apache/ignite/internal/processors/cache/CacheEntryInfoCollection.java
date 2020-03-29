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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class CacheEntryInfoCollection implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectCollection(GridCacheEntryInfo.class)
    private List<GridCacheEntryInfo> infos;

    /** */
    public CacheEntryInfoCollection() {
        // No-op
    }

    /**
     * @param infos List of cache entry info.
     */
    public CacheEntryInfoCollection(List<GridCacheEntryInfo> infos) {
        this.infos = infos;
    }

    /**
     *
     */
    public void init() {
        infos = new ArrayList<>();
    }

    /**
     * @return Entries.
     */
    public List<GridCacheEntryInfo> infos() {
        return infos;
    }

    /**
     * @param info Entry.
     */
    public void add(GridCacheEntryInfo info) {
        infos.add(info);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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
                if (!writer.writeCollection("infos", infos, MessageCollectionItemType.MSG))
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
                infos = reader.readCollection("infos", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheEntryInfoCollection.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 92;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");

        for (int i = 0; i < infos().size(); i++) {
            GridCacheEntryInfo info = infos().get(i);

            Object k = info.key().value(null, false);

            b.append("[key=").append(k == null ? "null" : k).append(", ver=").
                append(info.version()).append(", val=").append(info.value()).append(']');
        }

        b.append(']');

        return b.toString();
    }
}

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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class DataStreamerEntry implements Map.Entry<KeyCacheObject, CacheObject>, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    protected KeyCacheObject key;

    /** */
    @GridToStringInclude
    protected CacheObject val;

    /**
     *
     */
    public DataStreamerEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public DataStreamerEntry(KeyCacheObject key, CacheObject val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public CacheObject getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public CacheObject setValue(CacheObject val) {
        CacheObject old = this.val;

        this.val = val;

        return old;
    }

    /**
     * @param ctx Cache context.
     * @return Map entry unwrapping internal key and value.
     */
    public <K, V> Map.Entry<K, V> toEntry(final GridCacheContext ctx) {
        return new Map.Entry<K, V>() {
            @Override public K getKey() {
                return key.value(ctx.cacheObjectContext(), false);
            }

            @Override public V setValue(V val) {
                throw new UnsupportedOperationException();
            }

            @Override public V getValue() {
                return val != null ? val.<V>value(ctx.cacheObjectContext(), false) : null;
            }
        };
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
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("val", val))
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
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(DataStreamerEntry.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 95;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerEntry.class, this);
    }
}

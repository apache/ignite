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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class DataStreamerEntry implements Message {
    /** */
    @GridToStringInclude
    @Order(value = 0, method = "entryKey")
    protected KeyCacheObject key;

    /** */
    @GridToStringInclude
    @Order(value = 1, method = "value")
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

    /**
     * @return Key.
     */
    public KeyCacheObject entryKey() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void entryKey(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val Cache Value.
     */
    public void value(CacheObject val) {
        this.val = val;
    }

    /**
     * @param ctx Cache context.
     * @return Map entry unwrapping internal key and value.
     */
    public <K, V> Map.Entry<K, V> toEntry(final GridCacheContext ctx, final boolean keepBinary) {
        return new Map.Entry<K, V>() {
            @Override public K getKey() {
                return (K)ctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false, null);
            }

            @Override public V setValue(V val) {
                throw new UnsupportedOperationException();
            }

            @Override public V getValue() {
                return (V)ctx.cacheObjectContext().unwrapBinaryIfNeeded(val, keepBinary, false, null);
            }
        };
    }

    /**
     * @return Map entry unwrapping internal key and value.
     */
    public <K, V> Map.Entry<K, V> toEntry() {
        return new Map.Entry<K, V>() {
            @Override public K getKey() {
                return (K)entryKey();
            }

            @Override public V setValue(V val) {
                throw new UnsupportedOperationException();
            }

            @Override public V getValue() {
                return (V)value();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        // TODO: Safe to remove only after all inheritors have migrated to the new ser/der scheme (IGNITE-25490).
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeKeyCacheObject(key))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCacheObject(val))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        // TODO: Safe to remove only after all inheritors have migrated to the new ser/der scheme (IGNITE-25490).
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                key = reader.readKeyCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                val = reader.readCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 95;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerEntry.class, this);
    }
}

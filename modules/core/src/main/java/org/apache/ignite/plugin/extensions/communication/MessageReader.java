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

package org.apache.ignite.plugin.extensions.communication;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Communication message reader.
 * <p>
 * Allows to customize the binary format of communication messages.
 */
public interface MessageReader {
    /**
     * Sets but buffer to read from.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Reads {@code byte} value.
     *
     * @return {@code byte} value.
     */
    public byte readByte();

    /**
     * Reads {@code short} value.
     *
     * @return {@code short} value.
     */
    public short readShort();

    /**
     * Reads {@code int} value.
     *
     * @return {@code int} value.
     */
    public int readInt();

    /**
     * Reads {@code long} value.
     *
     * @return {@code long} value.
     */
    public long readLong();

    /**
     * Reads {@code float} value.
     *
     * @return {@code float} value.
     */
    public float readFloat();

    /**
     * Reads {@code double} value.
     *
     * @return {@code double} value.
     */
    public double readDouble();

    /**
     * Reads {@code char} value.
     *
     * @return {@code char} value.
     */
    public char readChar();

    /**
     * Reads {@code boolean} value.
     *
     * @return {@code boolean} value.
     */
    public boolean readBoolean();

    /**
     * Reads {@code byte} array.
     *
     * @return {@code byte} array.
     */
    public byte[] readByteArray();

    /**
     * Reads {@code short} array.
     *
     * @return {@code short} array.
     */
    public short[] readShortArray();

    /**
     * Reads {@code int} array.
     *
     * @return {@code int} array.
     */
    public int[] readIntArray();

    /**
     * Reads {@code long} array.
     *
     * @return {@code long} array.
     */
    public long[] readLongArray();

    /**
     * Reads {@code float} array.
     *
     * @return {@code float} array.
     */
    public float[] readFloatArray();

    /**
     * Reads {@code double} array.
     *
     * @return {@code double} array.
     */
    public double[] readDoubleArray();

    /**
     * Reads {@code char} array.
     *
     * @return {@code char} array.
     */
    public char[] readCharArray();

    /**
     * Reads {@code boolean} array.
     *
     * @return {@code boolean} array.
     */
    public boolean[] readBooleanArray();

    /**
     * Reads {@link String}.
     *
     * @return {@link String}.
     */
    public String readString();

    /**
     * Reads {@link BitSet}.
     *
     * @return {@link BitSet}.
     */
    public BitSet readBitSet();

    /**
     * Reads {@link UUID}.
     *
     * @return {@link UUID}.
     */
    public UUID readUuid();

    /**
     * Reads {@link IgniteUuid}.
     *
     * @return {@link IgniteUuid}.
     */
    public IgniteUuid readIgniteUuid();

    /**
     * Reads {@link AffinityTopologyVersion}.
     *
     * @return {@link AffinityTopologyVersion}.
     */
    public AffinityTopologyVersion readAffinityTopologyVersion();

    /**
     * Reads nested message.
     *
     * @param <T> Type of the message.
     * @return Message.
     */
    public <T extends Message> T readMessage();

    /**
     * Reads {@link CacheObject}.
     *
     * @return Cache object.
     */
    public CacheObject readCacheObject();

    /**
     * Reads {@link KeyCacheObject}.
     *
     * @return Key cache object.
     */
    public KeyCacheObject readKeyCacheObject();

    /**
     * Reads array of objects.
     *
     * @param itemType Array component type.
     * @param itemCls Array component class.
     * @param <T> Type of the red object .
     * @return Array of objects.
     */
    public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls);

    /**
     * Reads collection.
     *
     * @param itemType Collection item type.
     * @param <C> Type of the red collection.
     * @return Collection.
     */
    public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType);

    /**
     * Reads map.
     *
     * @param keyType Map key type.
     * @param valType Map value type.
     * @param linked Whether {@link LinkedHashMap} should be created.
     * @param <M> Type of the red map.
     * @return Map.
     */
    public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType,
        MessageCollectionItemType valType, boolean linked);

    /**
     * Tells whether last invocation of any of {@code readXXX(...)}
     * methods has fully written the value. {@code False} is returned
     * if there were not enough remaining bytes in byte buffer.
     *
     * @return Whether las value was fully read.
     */
    public boolean isLastRead();

    /**
     * Gets current read state.
     *
     * @return Read state.
     */
    public int state();

    /**
     * Increments read state.
     */
    public void incrementState();

    /**
     * Callback called before inner message is read.
     */
    public void beforeInnerMessageRead();

    /**
     * Callback called after inner message is read.
     *
     * @param finished Whether message was fully read.
     */
    public void afterInnerMessageRead(boolean finished);

    /**
     * Resets this reader.
     */
    public void reset();
}

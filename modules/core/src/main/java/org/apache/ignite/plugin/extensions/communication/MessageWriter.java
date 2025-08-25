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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Communication message writer.
 * <p>
 * Allows to customize the binary format of communication messages.
 */
public interface MessageWriter {
    /**
     * Sets but buffer to write to.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Writes message header.
     *
     * @param type Message type.
     * @return {@code true} if successfully. Otherwise returns {@code false}.
     */
    public boolean writeHeader(short type);

    /**
     * Writes {@code byte} value.
     *
     * @param val {@code byte} value.
     * @return Whether value was fully written.
     */
    public boolean writeByte(byte val);

    /**
     * Writes {@code short} value.
     *
     * @param val {@code short} value.
     * @return Whether value was fully written.
     */
    public boolean writeShort(short val);

    /**
     * Writes {@code int} value.
     *
     * @param val {@code int} value.
     * @return Whether value was fully written.
     */
    public boolean writeInt(int val);

    /**
     * Writes {@code long} value.
     *
     * @param val {@code long} value.
     * @return Whether value was fully written.
     */
    public boolean writeLong(long val);

    /**
     * Writes {@code float} value.
     *
     * @param val {@code float} value.
     * @return Whether value was fully written.
     */
    public boolean writeFloat(float val);

    /**
     * Writes {@code double} value.
     *
     * @param val {@code double} value.
     * @return Whether value was fully written.
     */
    public boolean writeDouble(double val);

    /**
     * Writes {@code char} value.
     *
     * @param val {@code char} value.
     * @return Whether value was fully written.
     */
    public boolean writeChar(char val);

    /**
     * Writes {@code boolean} value.
     *
     * @param val {@code boolean} value.
     * @return Whether value was fully written.
     */
    public boolean writeBoolean(boolean val);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @return Whether array was fully written.
     */
    public boolean writeByteArray(byte[] val);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     * @return Whether array was fully written.
     */
    public boolean writeByteArray(byte[] val, long off, int len);

    /**
     * Writes {@code short} array.
     *
     * @param val {@code short} array.
     * @return Whether array was fully written.
     */
    public boolean writeShortArray(short[] val);

    /**
     * Writes {@code int} array.
     *
     * @param val {@code int} array.
     * @return Whether array was fully written.
     */
    public boolean writeIntArray(int[] val);

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @return Whether array was fully written.
     */
    public boolean writeLongArray(long[] val);

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @param len Length.
     * @return Whether array was fully written.
     */
    public boolean writeLongArray(long[] val, int len);

    /**
     * Writes {@code float} array.
     *
     * @param val {@code float} array.
     * @return Whether array was fully written.
     */
    public boolean writeFloatArray(float[] val);

    /**
     * Writes {@code double} array.
     *
     * @param val {@code double} array.
     * @return Whether array was fully written.
     */
    public boolean writeDoubleArray(double[] val);

    /**
     * Writes {@code char} array.
     *
     * @param val {@code char} array.
     * @return Whether array was fully written.
     */
    public boolean writeCharArray(char[] val);

    /**
     * Writes {@code boolean} array.
     *
     * @param val {@code boolean} array.
     * @return Whether array was fully written.
     */
    public boolean writeBooleanArray(boolean[] val);

    /**
     * Writes {@link String}.
     *
     * @param val {@link String}.
     * @return Whether value was fully written.
     */
    public boolean writeString(String val);

    /**
     * Writes {@link BitSet}.
     *
     * @param val {@link BitSet}.
     * @return Whether value was fully written.
     */
    public boolean writeBitSet(BitSet val);

    /**
     * Writes {@link UUID}.
     *
     * @param val {@link UUID}.
     * @return Whether value was fully written.
     */
    public boolean writeUuid(UUID val);

    /**
     * Writes {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid}.
     * @return Whether value was fully written.
     */
    public boolean writeIgniteUuid(IgniteUuid val);

    /**
     * Writes {@link AffinityTopologyVersion}.
     *
     * @param val {@link AffinityTopologyVersion}.
     * @return Whether value was fully written.
     */
    public boolean writeAffinityTopologyVersion(AffinityTopologyVersion val);

    /**
     * Writes nested message.
     *
     * @param val Message.
     * @return Whether value was fully written.
     */
    public boolean writeMessage(Message val);

    /**
     * Writes {@link CacheObject}.
     *
     * @param obj Cache object.
     * @return Whether value was fully written.
     */
    public boolean writeCacheObject(CacheObject obj);

    /**
     * Writes {@link KeyCacheObject}.
     *
     * @param obj Key cache object.
     * @return Whether value was fully written.
     */
    public boolean writeKeyCacheObject(KeyCacheObject obj);

    /**
     * Writes array of objects.
     *
     * @param arr Array of objects.
     * @param itemType Array component type.
     * @param <T> Type of the objects that array contains.
     * @return Whether array was fully written.
     */
    public <T> boolean writeObjectArray(T[] arr, MessageCollectionItemType itemType);

    /**
     * Writes collection.
     *
     * @param col Collection.
     * @param itemType Collection item type.
     * @param <T> Type of the objects that collection contains.
     * @return Whether value was fully written.
     */
    public <T> boolean writeCollection(Collection<T> col, MessageCollectionItemType itemType);

    /**
     * Writes map.
     *
     * @param map Map.
     * @param keyType Map key type.
     * @param valType Map value type.
     * @param <K> Initial key types of the map to write.
     * @param <V> Initial value types of the map to write.
     * @return Whether value was fully written.
     */
    public <K, V> boolean writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType);

    /**
     * @return Whether header of current message is already written.
     */
    public boolean isHeaderWritten();

    /**
     * Callback called when header of the message is written.
     */
    public void onHeaderWritten();

    /**
     * Gets current message state.
     *
     * @return State.
     */
    public int state();

    /**
     * Increments state.
     */
    public void incrementState();

    /**
     * Callback called before inner message is written.
     */
    public void beforeInnerMessageWrite();

    /**
     * Callback called after inner message is written.
     *
     * @param finished Whether message was fully written.
     */
    public void afterInnerMessageWrite(boolean finished);

    /**
     * Resets this writer.
     */
    public void reset();
}

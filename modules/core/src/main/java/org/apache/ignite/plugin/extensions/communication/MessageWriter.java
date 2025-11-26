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
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

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
    @Deprecated
    public default void setBuffer(ByteBuffer buf) {
        // No-op.
    }

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
    public default boolean writeByte(byte val) {
        return writeByte(val, false);
    }

    /**
     * Writes {@code byte} value.
     *
     * @param val {@code byte} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeByte(byte val, boolean compress);

    /**
     * Writes {@code short} value.
     *
     * @param val {@code short} value.
     * @return Whether value was fully written.
     */
    public default boolean writeShort(short val) {
        return writeShort(val, false);
    }

    /**
     * Writes {@code short} value.
     *
     * @param val {@code short} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeShort(short val, boolean compress);

    /**
     * Writes {@code int} value.
     *
     * @param val {@code int} value.
     * @return Whether value was fully written.
     */
    public default boolean writeInt(int val) {
        return writeInt(val, false);
    }

    /**
     * Writes {@code int} value.
     *
     * @param val {@code int} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeInt(int val, boolean compress);

    /**
     * Writes {@code long} value.
     *
     * @param val {@code long} value.
     * @return Whether value was fully written.
     */
    public default boolean writeLong(long val) {
        return writeLong(val, false);
    }

    /**
     * Writes {@code long} value.
     *
     * @param val {@code long} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeLong(long val, boolean compress);

    /**
     * Writes {@code float} value.
     *
     * @param val {@code float} value.
     * @return Whether value was fully written.
     */
    public default boolean writeFloat(float val) {
        return writeFloat(val, false);
    }

    /**
     * Writes {@code float} value.
     *
     * @param val {@code float} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeFloat(float val, boolean compress);

    /**
     * Writes {@code double} value.
     *
     * @param val {@code double} value.
     * @return Whether value was fully written.
     */
    public default boolean writeDouble(double val) {
        return writeDouble(val, false);
    }

    /**
     * Writes {@code double} value.
     *
     * @param val {@code double} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeDouble(double val, boolean compress);

    /**
     * Writes {@code char} value.
     *
     * @param val {@code char} value.
     * @return Whether value was fully written.
     */
    public default boolean writeChar(char val) {
        return writeChar(val, false);
    }

    /**
     * Writes {@code char} value.
     *
     * @param val {@code char} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeChar(char val, boolean compress);

    /**
     * Writes {@code boolean} value.
     *
     * @param val {@code boolean} value.
     * @return Whether value was fully written.
     */
    public default boolean writeBoolean(boolean val) {
        return writeBoolean(val, false);
    }

    /**
     * Writes {@code boolean} value.
     *
     * @param val {@code boolean} value.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeBoolean(boolean val, boolean compress);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @return Whether array was fully written.
     */
    public default boolean writeByteArray(byte[] val) {
        return writeByteArray(val, false);
    }

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeByteArray(byte[] val, boolean compress);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     * @return Whether array was fully written.
     */
    public default boolean writeByteArray(byte[] val, long off, int len) {
        return writeByteArray(val, off, len, false);
    }

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeByteArray(byte[] val, long off, int len, boolean compress);

    /**
     * Writes {@code short} array.
     *
     * @param val {@code short} array.
     * @return Whether array was fully written.
     */
    public default boolean writeShortArray(short[] val) {
        return writeShortArray(val, false);
    }

    /**
     * Writes {@code short} array.
     *
     * @param val {@code short} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeShortArray(short[] val, boolean compress);

    /**
     * Writes {@code int} array.
     *
     * @param val {@code int} array.
     * @return Whether array was fully written.
     */
    public default boolean writeIntArray(int[] val) {
        return writeIntArray(val, false);
    }

    /**
     * Writes {@code int} array.
     *
     * @param val {@code int} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeIntArray(int[] val, boolean compress);

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @return Whether array was fully written.
     */
    public default boolean writeLongArray(long[] val) {
        return writeLongArray(val, false);
    }

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeLongArray(long[] val, boolean compress);

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @param len Length.
     * @return Whether array was fully written.
     */
    public default boolean writeLongArray(long[] val, int len) {
        return writeLongArray(val, len, false);
    }

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     * @param len Length.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeLongArray(long[] val, int len, boolean compress);

    /**
     * Writes {@code float} array.
     *
     * @param val {@code float} array.
     * @return Whether array was fully written.
     */
    public default boolean writeFloatArray(float[] val) {
        return writeFloatArray(val, false);
    }

    /**
     * Writes {@code float} array.
     *
     * @param val {@code float} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeFloatArray(float[] val, boolean compress);

    /**
     * Writes {@code double} array.
     *
     * @param val {@code double} array.
     * @return Whether array was fully written.
     */
    public default boolean writeDoubleArray(double[] val) {
        return writeDoubleArray(val, false);
    }

    /**
     * Writes {@code double} array.
     *
     * @param val {@code double} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeDoubleArray(double[] val, boolean compress);

    /**
     * Writes {@code char} array.
     *
     * @param val {@code char} array.
     * @return Whether array was fully written.
     */
    public default boolean writeCharArray(char[] val) {
        return writeCharArray(val, false);
    }

    /**
     * Writes {@code char} array.
     *
     * @param val {@code char} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeCharArray(char[] val, boolean compress);

    /**
     * Writes {@code boolean} array.
     *
     * @param val {@code boolean} array.
     * @return Whether array was fully written.
     */
    public default boolean writeBooleanArray(boolean[] val) {
        return writeBooleanArray(val, false);
    }

    /**
     * Writes {@code boolean} array.
     *
     * @param val {@code boolean} array.
     * @param compress
     * @return Whether array was fully written.
     */
    public boolean writeBooleanArray(boolean[] val, boolean compress);

    /**
     * Writes {@link String}.
     *
     * @param val {@link String}.
     * @return Whether value was fully written.
     */
    public default boolean writeString(String val) {
        return writeString(val, false);
    }

    /**
     * Writes {@link String}.
     *
     * @param val {@link String}.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeString(String val, boolean compress);

    /**
     * Writes {@link BitSet}.
     *
     * @param val {@link BitSet}.
     * @return Whether value was fully written.
     */
    public default boolean writeBitSet(BitSet val) {
        return writeBitSet(val, false);
    }

    /**
     * Writes {@link BitSet}.
     *
     * @param val {@link BitSet}.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeBitSet(BitSet val, boolean compress);

    /**
     * Writes {@link UUID}.
     *
     * @param val {@link UUID}.
     * @return Whether value was fully written.
     */
    public default boolean writeUuid(UUID val) {
        return writeUuid(val, false);
    }

    /**
     * Writes {@link UUID}.
     *
     * @param val {@link UUID}.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeUuid(UUID val, boolean compress);

    /**
     * Writes {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid}.
     * @return Whether value was fully written.
     */
    public default boolean writeIgniteUuid(IgniteUuid val) {
        return writeIgniteUuid(val, false);
    }

    /**
     * Writes {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid}.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeIgniteUuid(IgniteUuid val, boolean compress);

    /**
     * Writes {@link AffinityTopologyVersion}.
     *
     * @param val {@link AffinityTopologyVersion}.
     * @return Whether value was fully written.
     */
    public default boolean writeAffinityTopologyVersion(AffinityTopologyVersion val) {
        return writeAffinityTopologyVersion(val, false);
    }

    /**
     * Writes {@link AffinityTopologyVersion}.
     *
     * @param val {@link AffinityTopologyVersion}.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeAffinityTopologyVersion(AffinityTopologyVersion val, boolean compress);

    /**
     * Writes nested message.
     *
     * @param val Message.
     * @return Whether value was fully written.
     */
    public default boolean writeMessage(Message val) {
        return writeMessage(val, false);
    }

    /**
     * Writes nested message.
     *
     * @param val Message.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeMessage(Message val, boolean compress);

    /**
     * Writes {@link CacheObject}.
     *
     * @param obj Cache object.
     * @return Whether value was fully written.
     */
    public default boolean writeCacheObject(CacheObject obj) {
        return writeCacheObject(obj, false);
    }

    /**
     * Writes {@link CacheObject}.
     *
     * @param obj Cache object.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeCacheObject(CacheObject obj, boolean compress);

    /**
     * Writes {@link KeyCacheObject}.
     *
     * @param obj Key cache object.
     * @return Whether value was fully written.
     */
    public default boolean writeKeyCacheObject(KeyCacheObject obj) {
        return writeKeyCacheObject(obj, false);
    }

    /**
     * Writes {@link KeyCacheObject}.
     *
     * @param obj Key cache object.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeKeyCacheObject(KeyCacheObject obj, boolean compress);

    /**
     * Writes {@link GridLongList}.
     *
     * @param ll Grid long list.
     * @return Whether value was fully written.
     */
    public default boolean writeGridLongList(@Nullable GridLongList ll) {
        return writeGridLongList(ll, false);
    }

    /**
     * Writes {@link GridLongList}.
     *
     * @param ll Grid long list.
     * @param compress
     * @return Whether value was fully written.
     */
    public boolean writeGridLongList(@Nullable GridLongList ll, boolean compress);

    /**
     * Writes array of objects.
     *
     * @param arr Array of objects.
     * @param itemType Array component type.
     * @param <T> Type of the objects that array contains.
     * @return Whether array was fully written.
     */
    public default <T> boolean writeObjectArray(T[] arr, MessageCollectionItemType itemType) {
        return writeObjectArray(arr, itemType, false);
    }

    /**
     * Writes array of objects.
     *
     * @param arr Array of objects.
     * @param itemType Array component type.
     * @param compress
     * @param <T> Type of the objects that array contains.
     * @return Whether array was fully written.
     */
    public <T> boolean writeObjectArray(T[] arr, MessageCollectionItemType itemType, boolean compress);

    /**
     * Writes collection.
     *
     * @param col Collection.
     * @param itemType Collection item type.
     * @param <T> Type of the objects that collection contains.
     * @return Whether value was fully written.
     */
    public default <T> boolean writeCollection(Collection<T> col, MessageCollectionItemType itemType) {
        return writeCollection(col, itemType, false);
    }

    /**
     * Writes collection.
     *
     * @param col Collection.
     * @param itemType Collection item type.
     * @param compress
     * @param <T> Type of the objects that collection contains.
     * @return Whether value was fully written.
     */
    public <T> boolean writeCollection(Collection<T> col, MessageCollectionItemType itemType, boolean compress);

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
    public default <K, V> boolean writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType) {
        return writeMap(map, keyType, valType, false);
    }

    /**
     * Writes map.
     *
     * @param map Map.
     * @param keyType Map key type.
     * @param valType Map value type.
     * @param compress
     * @param <K> Initial key types of the map to write.
     * @param <V> Initial value types of the map to write.
     * @return Whether value was fully written.
     */
    public <K, V> boolean writeMap(Map<K, V> map, MessageCollectionItemType keyType,
                                   MessageCollectionItemType valType, boolean compress);

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

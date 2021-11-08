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

package org.apache.ignite.internal.network.direct.stream;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageReader;
import org.apache.ignite.network.serialization.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Direct marshalling I/O stream.
 */
public interface DirectByteBufferStream {
    /**
     * Sets buffer.
     *
     * @param buf Buffer.
     */
    void setBuffer(ByteBuffer buf);

    /**
     * Returns number of remaining bytes.
     *
     * @return Number of remaining bytes.
     */
    int remaining();

    /**
     * Returns whether last object was fully written or read.
     *
     * @return Whether last object was fully written or read.
     */
    boolean lastFinished();

    /**
     * Writes {@code byte}.
     *
     * @param val Value.
     */
    void writeByte(byte val);

    /**
     * Writes {@code short}.
     *
     * @param val Value.
     */
    void writeShort(short val);

    /**
     * Writes {@code int}.
     *
     * @param val Value.
     */
    void writeInt(int val);

    /**
     * Writes {@code long}.
     *
     * @param val Value.
     */
    void writeLong(long val);

    /**
     * Writes {@code float}.
     *
     * @param val Value.
     */
    void writeFloat(float val);

    /**
     * Writes {@code double}.
     *
     * @param val Value.
     */
    void writeDouble(double val);

    /**
     * Writes {@code char}.
     *
     * @param val Value.
     */
    void writeChar(char val);

    /**
     * Writes {@code boolean}.
     *
     * @param val Value.
     */
    void writeBoolean(boolean val);

    /**
     * Writes {@code byte} array.
     *
     * @param val Value.
     */
    void writeByteArray(byte[] val);

    /**
     * Writes {@code byte} array.
     *
     * @param val Value.
     * @param off Offset.
     * @param len Length.
     */
    void writeByteArray(byte[] val, long off, int len);

    /**
     * Writes {@code short} array.
     *
     * @param val Value.
     */
    void writeShortArray(short[] val);

    /**
     * Writes {@code int} array.
     *
     * @param val Value.
     */
    void writeIntArray(int[] val);

    /**
     * Writes {@code long} array.
     *
     * @param val Value.
     */
    void writeLongArray(long[] val);

    /**
     * Writes {@code long} array.
     *
     * @param val Value.
     * @param len Length.
     */
    void writeLongArray(long[] val, int len);

    /**
     * Writes {@code float} array.
     *
     * @param val Value.
     */
    void writeFloatArray(float[] val);

    /**
     * Writes {@code double} array.
     *
     * @param val Value.
     */
    void writeDoubleArray(double[] val);

    /**
     * Writes {@code char} array.
     *
     * @param val Value.
     */
    void writeCharArray(char[] val);

    /**
     * Writes {@code boolean} array.
     *
     * @param val Value.
     */
    void writeBooleanArray(boolean[] val);

    /**
     * Writes {@link String}.
     *
     * @param val Value.
     */
    void writeString(String val);

    /**
     * Writes {@link BitSet}.
     *
     * @param val Value.
     */
    void writeBitSet(BitSet val);

    /**
     * Writes {@link UUID}.
     *
     * @param val Value.
     */
    void writeUuid(UUID val);

    /**
     * Writes {@link IgniteUuid}.
     *
     * @param val Value.
     */
    void writeIgniteUuid(IgniteUuid val);

    /**
     * Writes {@link NetworkMessage}.
     *
     * @param msg    Message.
     * @param writer Writer.
     */
    void writeMessage(NetworkMessage msg, MessageWriter writer);

    /**
     * Writes {@link Object} array.
     *
     * @param <T>      Type of the array.
     * @param arr      Array.
     * @param itemType Component type.
     * @param writer   Writer.
     */
    <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType, MessageWriter writer);

    /**
     * Writes {@link Collection}.
     *
     * @param <T>      Type of the collection.
     * @param col      Collection.
     * @param itemType Component type.
     * @param writer   Writer.
     */
    <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType, MessageWriter writer);

    /**
     * Writes {@link Map}.
     *
     * @param <K>     Type of the map's keys.
     * @param <V>     Type of the map's values.
     * @param map     Map.
     * @param keyType Key type.
     * @param valType Value type.
     * @param writer  Writer.
     */
    <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType, MessageCollectionItemType valType,
            MessageWriter writer);

    /**
     * Reads {@code byte}.
     *
     * @return Value.
     */
    byte readByte();

    /**
     * Reads {@code short}.
     *
     * @return Value.
     */
    short readShort();

    /**
     * Reads {@code int}.
     *
     * @return Value.
     */
    int readInt();

    /**
     * Reads {@code long}.
     *
     * @return Value.
     */
    long readLong();

    /**
     * Reads {@code float}.
     *
     * @return Value.
     */
    float readFloat();

    /**
     * Reads {@code double}.
     *
     * @return Value.
     */
    double readDouble();

    /**
     * Reads {@code char}.
     *
     * @return Value.
     */
    char readChar();

    /**
     * Reads {@code boolean}.
     *
     * @return Value.
     */
    boolean readBoolean();

    /**
     * Reads {@code byte} array.
     *
     * @return Value.
     */
    byte[] readByteArray();

    /**
     * Reads {@code short} array.
     *
     * @return Value.
     */
    short[] readShortArray();

    /**
     * Reads {@code int} array.
     *
     * @return Value.
     */
    int[] readIntArray();

    /**
     * Reads {@code long} array.
     *
     * @return Value.
     */
    long[] readLongArray();

    /**
     * Reads {@code float} array.
     *
     * @return Value.
     */
    float[] readFloatArray();

    /**
     * Reads {@code double} array.
     *
     * @return Value.
     */
    double[] readDoubleArray();

    /**
     * Reads {@code char} array.
     *
     * @return Value.
     */
    char[] readCharArray();

    /**
     * Reads {@code boolean} array.
     *
     * @return Value.
     */
    boolean[] readBooleanArray();

    /**
     * Reads {@link String}.
     *
     * @return Value.
     */
    String readString();

    /**
     * Reads {@link BitSet}.
     *
     * @return Value.
     */
    BitSet readBitSet();

    /**
     * Reads {@link UUID}.
     *
     * @return Value.
     */
    UUID readUuid();

    /**
     * Reads {@link IgniteUuid}.
     *
     * @return Value.
     */
    IgniteUuid readIgniteUuid();

    /**
     * Reads {@link NetworkMessage}.
     *
     * @param <T>    Type of message.
     * @param reader Reader.
     * @return Message.
     */
    <T extends NetworkMessage> T readMessage(MessageReader reader);

    /**
     * Reads {@link Object} array.
     *
     * @param <T>      Type of array.
     * @param itemType Item type.
     * @param itemCls  Item class.
     * @param reader   Reader.
     * @return Array.
     */
    <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls, MessageReader reader);

    /**
     * Reads {@link Collection}.
     *
     * @param <C>      Type of collection.
     * @param itemType Item type.
     * @param reader   Reader.
     * @return Collection.
     */
    <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType, MessageReader reader);

    /**
     * Reads {@link Map}.
     *
     * @param <M>     Type of map.
     * @param keyType Key type.
     * @param valType Value type.
     * @param linked  Whether linked map should be created.
     * @param reader  Reader.
     * @return Map.
     */
    <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType, MessageCollectionItemType valType,
            boolean linked, MessageReader reader);
}

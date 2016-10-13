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

package org.apache.ignite.plugin.extensions.communication.opto;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Communication message writer.
 * <p>
 * Allows to customize the binary format of communication messages.
 */
public interface OptimizedMessageWriter {
    /**
     * Writes message header.
     *
     * @param type Message type.
     */
    public void writeHeader(byte type);

    /**
     * Writes {@code byte} value.
     *
     * @param val {@code byte} value.
     */
    public void writeByte(byte val);

    /**
     * Writes {@code short} value.
     *
     * @param val {@code short} value.
     */
    public void writeShort(short val);

    /**
     * Writes {@code int} value.
     *
     * @param val {@code int} value.
     */
    public void writeInt(int val);

    /**
     * Writes {@code long} value.
     *
     * @param val {@code long} value.
     */
    public void writeLong(long val);

    /**
     * Writes {@code float} value.
     *
     * @param val {@code float} value.
     */
    public void writeFloat(float val);

    /**
     * Writes {@code double} value.
     *
     * @param val {@code double} value.
     */
    public void writeDouble(double val);

    /**
     * Writes {@code char} value.
     *
     * @param val {@code char} value.
     */
    public void writeChar(char val);

    /**
     * Writes {@code boolean} value.
     *
     * @param val {@code boolean} value.
     */
    public void writeBoolean(boolean val);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     */
    public void writeByteArray(byte[] val);

    /**
     * Writes {@code byte} array.
     *
     * @param val {@code byte} array.
     * @param off Offset.
     * @param len Length.
     */
    public void writeByteArray(byte[] val, long off, int len);

    /**
     * Writes {@code short} array.
     *
     * @param val {@code short} array.
     */
    public void writeShortArray(short[] val);

    /**
     * Writes {@code int} array.
     *
     * @param val {@code int} array.
     */
    public void writeIntArray(int[] val);

    /**
     * Writes {@code long} array.
     *
     * @param val {@code long} array.
     */
    public void writeLongArray(long[] val);

    /**
     * Writes {@code float} array.
     *
     * @param val {@code float} array.
     */
    public void writeFloatArray(float[] val);

    /**
     * Writes {@code double} array.
     *
     * @param val {@code double} array.
     */
    public void writeDoubleArray(double[] val);

    /**
     * Writes {@code char} array.
     *
     * @param val {@code char} array.
     */
    public void writeCharArray(char[] val);

    /**
     * Writes {@code boolean} array.
     *
     * @param val {@code boolean} array.
     */
    public void writeBooleanArray(boolean[] val);

    /**
     * Writes {@link String}.
     *
     * @param val {@link String}.
     */
    public void writeString(String val);

    /**
     * Writes {@link BitSet}.
     *
     * @param val {@link BitSet}.
     */
    public void writeBitSet(BitSet val);

    /**
     * Writes {@link UUID}.
     *
     * @param val {@link UUID}.
     */
    public void writeUuid(UUID val);

    /**
     * Writes {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid}.
     */
    public void writeIgniteUuid(IgniteUuid val);

    /**
     * Writes nested message.
     *
     * @param val Message.
     */
    public void writeMessage(OptimizedMessage val);

    /**
     * Writes array of objects.
     *
     * @param arr Array of objects.
     * @param itemType Array component type.
     */
    public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType);

    /**
     * Writes collection.
     *
     * @param col Collection.
     * @param itemType Collection item type.
     */
    public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType);

    /**
     * Writes map.
     *
     * @param map Map.
     * @param keyType Map key type.
     * @param valType Map value type.
     */
    public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType);
}

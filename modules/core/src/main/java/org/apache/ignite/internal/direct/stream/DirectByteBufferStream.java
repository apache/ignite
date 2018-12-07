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

package org.apache.ignite.internal.direct.stream;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Direct marshalling I/O stream.
 */
public interface DirectByteBufferStream {
    /**
     * @param buf Buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * @return Number of remaining bytes.
     */
    public int remaining();

    /**
     * @return Whether last object was fully written or read.
     */
    public boolean lastFinished();

    /**
     * @param val Value.
     */
    public void writeByte(byte val);

    /**
     * @param val Value.
     */
    public void writeShort(short val);

    /**
     * @param val Value.
     */
    public void writeInt(int val);

    /**
     * @param val Value.
     */
    public void writeLong(long val);

    /**
     * @param val Value.
     */
    public void writeFloat(float val);

    /**
     * @param val Value.
     */
    public void writeDouble(double val);

    /**
     * @param val Value.
     */
    public void writeChar(char val);

    /**
     * @param val Value.
     */
    public void writeBoolean(boolean val);

    /**
     * @param val Value.
     */
    public void writeByteArray(byte[] val);

    /**
     * @param val Value.
     * @param off Offset.
     * @param len Length.
     */
    public void writeByteArray(byte[] val, long off, int len);

    /**
     * @param val Value.
     */
    public void writeShortArray(short[] val);

    /**
     * @param val Value.
     */
    public void writeIntArray(int[] val);

    /**
     * @param val Value.
     */
    public void writeLongArray(long[] val);

    /**
     * @param val Value.
     * @param len Length.
     */
    public void writeLongArray(long[] val, int len);

    /**
     * @param val Value.
     */
    public void writeFloatArray(float[] val);

    /**
     * @param val Value.
     */
    public void writeDoubleArray(double[] val);

    /**
     * @param val Value.
     */
    public void writeCharArray(char[] val);

    /**
     * @param val Value.
     */
    public void writeBooleanArray(boolean[] val);

    /**
     * @param val Value.
     */
    public void writeString(String val);

    /**
     * @param val Value.
     */
    public void writeBitSet(BitSet val);

    /**
     * @param val Value.
     */
    public void writeUuid(UUID val);

    /**
     * @param val Value.
     */
    public void writeIgniteUuid(IgniteUuid val);

    /**
     * @param val Value.
     */
    public void writeAffinityTopologyVersion(AffinityTopologyVersion val);

    /**
     * @param msg Message.
     * @param writer Writer.
     */
    public void writeMessage(Message msg, MessageWriter writer);

    /**
     * @param arr Array.
     * @param itemType Component type.
     * @param writer Writer.
     */
    public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType, MessageWriter writer);

    /**
     * @param col Collection.
     * @param itemType Component type.
     * @param writer Writer.
     */
    public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType, MessageWriter writer);

    /**
     * @param map Map.
     * @param keyType Key type.
     * @param valType Value type.
     * @param writer Writer.
     */
    public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType, MessageCollectionItemType valType,
        MessageWriter writer);

    /**
     * @return Value.
     */
    public byte readByte();

    /**
     * @return Value.
     */
    public short readShort();

    /**
     * @return Value.
     */
    public int readInt();

    /**
     * @return Value.
     */
    public long readLong();

    /**
     * @return Value.
     */
    public float readFloat();

    /**
     * @return Value.
     */
    public double readDouble();

    /**
     * @return Value.
     */
    public char readChar();

    /**
     * @return Value.
     */
    public boolean readBoolean();

    /**
     * @return Value.
     */
    public byte[] readByteArray();

    /**
     * @return Value.
     */
    public short[] readShortArray();

    /**
     * @return Value.
     */
    public int[] readIntArray();

    /**
     * @return Value.
     */
    public long[] readLongArray();

    /**
     * @return Value.
     */
    public float[] readFloatArray();

    /**
     * @return Value.
     */
    public double[] readDoubleArray();

    /**
     * @return Value.
     */
    public char[] readCharArray();

    /**
     * @return Value.
     */
    public boolean[] readBooleanArray();

    /**
     * @return Value.
     */
    public String readString();

    /**
     * @return Value.
     */
    public BitSet readBitSet();

    /**
     * @return Value.
     */
    public UUID readUuid();

    /**
     * @return Value.
     */
    public IgniteUuid readIgniteUuid();

    /**
     * @return Value.
     */
    public AffinityTopologyVersion readAffinityTopologyVersion();

    /**
     * @param reader Reader.
     * @return Message.
     */
    public <T extends Message> T readMessage(MessageReader reader);

    /**
     * @param itemType Item type.
     * @param itemCls Item class.
     * @param reader Reader.
     * @return Array.
     */
    public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls, MessageReader reader);

    /**
     * @param itemType Item type.
     * @param reader Reader.
     * @return Collection.
     */
    public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType, MessageReader reader);

    /**
     * @param keyType Key type.
     * @param valType Value type.
     * @param linked Whether linked map should be created.
     * @param reader Reader.
     * @return Map.
     */
    public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType, MessageCollectionItemType valType,
        boolean linked, MessageReader reader);
}

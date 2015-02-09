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

import org.apache.ignite.lang.*;

import java.nio.*;
import java.util.*;

/**
 * TODO
 */
public interface MessageReader {
    public void setBuffer(ByteBuffer buf);

    public byte readByte(String name);

    public short readShort(String name);

    public int readInt(String name);

    public long readLong(String name);

    public float readFloat(String name);

    public double readDouble(String name);

    public char readChar(String name);

    public boolean readBoolean(String name);

    public byte[] readByteArray(String name);

    public short[] readShortArray(String name);

    public int[] readIntArray(String name);

    public long[] readLongArray(String name);

    public float[] readFloatArray(String name);

    public double[] readDoubleArray(String name);

    public char[] readCharArray(String name);

    public boolean[] readBooleanArray(String name);

    public String readString(String name);

    public BitSet readBitSet(String name);

    public UUID readUuid(String name);

    public IgniteUuid readIgniteUuid(String name);

    public <T extends Enum<T>> T readEnum(String name, Class<T> enumCls);

    public <T extends MessageAdapter> T readMessage(String name);

    public <T> T[] readObjectArray(String name, Class<T> itemCls);

    public <C extends Collection<T>, T> C readCollection(String name, Class<T> itemCls);

    public <M extends Map<K, V>, K, V> M readMap(String name, Class<K> keyCls, Class<V> valCls, boolean linked);

    public boolean isLastRead();
}

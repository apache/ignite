/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.lang.*;

import java.nio.*;
import java.util.*;

/**
 * TODO
 */
public interface MessageWriter {
    public void setBuffer(ByteBuffer buf);

    public boolean writeByte(String name, byte val);

    public boolean writeShort(String name, short val);

    public boolean writeInt(String name, int val);

    public boolean writeLong(String name, long val);

    public boolean writeFloat(String name, float val);

    public boolean writeDouble(String name, double val);

    public boolean writeChar(String name, char val);

    public boolean writeBoolean(String name, boolean val);

    public boolean writeByteArray(String name, byte[] val);

    public boolean writeShortArray(String name, short[] val);

    public boolean writeIntArray(String name, int[] val);

    public boolean writeLongArray(String name, long[] val);

    public boolean writeFloatArray(String name, float[] val);

    public boolean writeDoubleArray(String name, double[] val);

    public boolean writeCharArray(String name, char[] val);

    public boolean writeBooleanArray(String name, boolean[] val);

    public boolean writeString(String name, String val);

    public boolean writeBitSet(String name, BitSet val);

    public boolean writeUuid(String name, UUID val);

    public boolean writeIgniteUuid(String name, IgniteUuid val);

    public boolean writeEnum(String name, Enum<?> val);

    public boolean writeMessage(String name, MessageAdapter val);

    public <T> boolean writeObjectArray(String name, T[] arr, Class<T> itemCls);

    public <T> boolean writeCollection(String name, Collection<T> col, Class<T> itemCls);

    public <K, V> boolean writeMap(String name, Map<K, V> map, Class<K> keyCls, Class<V> valCls);
}

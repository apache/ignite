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

package org.apache.ignite.internal.util.direct;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.clock.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Communication message adapter.
 */
public abstract class GridTcpCommunicationMessageAdapter implements Serializable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final byte[] BYTE_ARR_NOT_READ = new byte[0];

    /** */
    public static final short[] SHORT_ARR_NOT_READ = new short[0];

    /** */
    public static final int[] INT_ARR_NOT_READ = new int[0];

    /** */
    public static final long[] LONG_ARR_NOT_READ = new long[0];

    /** */
    public static final float[] FLOAT_ARR_NOT_READ = new float[0];

    /** */
    public static final double[] DOUBLE_ARR_NOT_READ = new double[0];

    /** */
    public static final char[] CHAR_ARR_NOT_READ = new char[0];

    /** */
    public static final boolean[] BOOLEAN_ARR_NOT_READ = new boolean[0];

    /** */
    public static final UUID UUID_NOT_READ = new UUID(0, 0);

    /** */
    public static final ByteBuffer BYTE_BUF_NOT_READ = ByteBuffer.allocate(0);

    /** */
    public static final IgniteUuid GRID_UUID_NOT_READ = new IgniteUuid(new UUID(0, 0), 0);

    /** */
    public static final GridClockDeltaVersion CLOCK_DELTA_VER_NOT_READ = new GridClockDeltaVersion(0, 0);

    /** */
    public static final GridByteArrayList BYTE_ARR_LIST_NOT_READ = new GridByteArrayList(new byte[0]);

    /** */
    public static final GridLongList LONG_LIST_NOT_READ = new GridLongList(0);

    /** */
    public static final GridCacheVersion CACHE_VER_NOT_READ = new GridCacheVersion(0, 0, 0, 0);

    /** */
    public static final GridDhtPartitionExchangeId DHT_PART_EXCHANGE_ID_NOT_READ =
        new GridDhtPartitionExchangeId(new UUID(0, 0), EVT_NODE_LEFT, 1);

    /** */
    public static final GridCacheValueBytes VAL_BYTES_NOT_READ = new GridCacheValueBytes();

    /** */
    @SuppressWarnings("RedundantStringConstructorCall")
    public static final String STR_NOT_READ = new String();

    /** */
    public static final BitSet BIT_SET_NOT_READ = new BitSet();

    /** */
    public static final GridTcpCommunicationMessageAdapter MSG_NOT_READ = new GridTcpCommunicationMessageAdapter() {
        @SuppressWarnings("CloneDoesntCallSuperClone")
        @Override public GridTcpCommunicationMessageAdapter clone() {
            throw new UnsupportedOperationException();
        }

        @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
            throw new UnsupportedOperationException();
        }

        @Override public boolean writeTo(ByteBuffer buf) {
            throw new UnsupportedOperationException();
        }

        @Override public boolean readFrom(ByteBuffer buf) {
            throw new UnsupportedOperationException();
        }

        @Override public byte directType() {
            throw new UnsupportedOperationException();
        }
    };

    /** */
    protected static final Object NULL = new Object();

    /** */
    protected final GridTcpCommunicationMessageState commState = new GridTcpCommunicationMessageState();

    /**
     * @param writer Writer.
     */
    public final void setWriter(MessageWriter writer) {
        assert writer != null;

        commState.setWriter(writer);
    }

    /**
     * @param reader Reader.
     */
    public final void setReader(MessageReader reader) {
        assert reader != null;

        commState.setReader(reader);
    }

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully written.
     */
    public abstract boolean writeTo(ByteBuffer buf);

    /**
     * @param buf Byte buffer.
     * @return Whether message was fully read.
     */
    public abstract boolean readFrom(ByteBuffer buf);

    /**
     * @return Message type.
     */
    public abstract byte directType();

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override public abstract GridTcpCommunicationMessageAdapter clone();

    /**
     * Clones all fields of the provided message to {@code this}.
     *
     * @param _msg Message to clone from.
     */
    protected abstract void clone0(GridTcpCommunicationMessageAdapter _msg);

    /**
     * @return {@code True} if should skip recovery for this message.
     */
    public boolean skipRecovery() {
        return false;
    }

    /**
     * @param arr Array.
     * @return Array iterator.
     */
    protected final Iterator<?> arrayIterator(final Object[] arr) {
        return new Iterator<Object>() {
            private int idx;

            @Override public boolean hasNext() {
                return idx < arr.length;
            }

            @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return arr[idx++];
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}

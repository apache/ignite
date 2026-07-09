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

package org.apache.ignite.internal.direct;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.direct.state.DirectMessageState;
import org.apache.ignite.internal.direct.state.DirectMessageStateItem;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.managers.communication.CompressedMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageMapType;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_NETWORK_COMPRESSION;

/**
 * Message writer implementation.
 */
public class DirectMessageWriter implements MessageWriter {
    /** Temporary buffer capacity.  */
    private static final int TMP_BUF_CAPACITY = 10 * 1024;

    /** State. */
    @GridToStringInclude
    private final DirectMessageState<StateItem> state;

    /** Message factory. */
    private final MessageFactory msgFactory;

    /** Compression level. Used only for {@link CompressedMessage}. */
    private final int compressionLvl;

    /** Buffer for writing. */
    private ByteBuffer buf;

    /**
     * Cached stream of the current state item. Avoids re-resolving {@code state.item().stream} on every primitive
     * write; updated only when the current item changes (buffer set / nested write enter or exit).
     */
    private DirectByteBufferStream curStream;

    /** Reusable writer for compressed payload serialization. Lazily created; thread-confined like the writer itself. */
    private DirectMessageWriter tmpWriter;

    /** Reusable scratch buffer for compressed payload serialization. Retained at the largest size seen so far. */
    private ByteBuffer tmpBuf;

    /** @param msgFactory Message factory. */
    public DirectMessageWriter(final MessageFactory msgFactory) {
        this(msgFactory, DFLT_NETWORK_COMPRESSION);
    }

    /**
     * @param msgFactory Message factory.
     * @param compressionLvl Compression level.
     */
    public DirectMessageWriter(final MessageFactory msgFactory, final int compressionLvl) {
        this.msgFactory = msgFactory;
        this.compressionLvl = compressionLvl;

        state = new DirectMessageState<>(StateItem.class, new IgniteOutClosure<StateItem>() {
            @Override public StateItem apply() {
                return new StateItem(msgFactory);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        this.buf = buf;

        curStream = state.item().stream;

        curStream.setBuffer(buf);
    }

    /**
     * Gets buffer to write to.
     *
     * @return Byte buffer.
     */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** {@inheritDoc} */
    @Override public boolean writeHeader(short type) {
        DirectByteBufferStream stream = curStream;

        stream.writeShort(type);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByte(byte val) {
        DirectByteBufferStream stream = curStream;

        stream.writeByte(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShort(short val) {
        DirectByteBufferStream stream = curStream;

        stream.writeShort(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeInt(int val) {
        DirectByteBufferStream stream = curStream;

        stream.writeInt(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLong(long val) {
        DirectByteBufferStream stream = curStream;

        stream.writeLong(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloat(float val) {
        DirectByteBufferStream stream = curStream;

        stream.writeFloat(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDouble(double val) {
        DirectByteBufferStream stream = curStream;

        stream.writeDouble(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeChar(char val) {
        DirectByteBufferStream stream = curStream;

        stream.writeChar(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBoolean(boolean val) {
        DirectByteBufferStream stream = curStream;

        stream.writeBoolean(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(@Nullable byte[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeByteArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(byte[] val, long off, int len) {
        DirectByteBufferStream stream = curStream;

        stream.writeByteArray(val, off, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShortArray(@Nullable short[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeShortArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIntArray(@Nullable int[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeIntArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(@Nullable long[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeLongArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(long[] val, int len) {
        DirectByteBufferStream stream = curStream;

        stream.writeLongArray(val, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloatArray(@Nullable float[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeFloatArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDoubleArray(@Nullable double[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeDoubleArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeCharArray(@Nullable char[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeCharArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBooleanArray(@Nullable boolean[] val) {
        DirectByteBufferStream stream = curStream;

        stream.writeBooleanArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeString(String val) {
        DirectByteBufferStream stream = curStream;

        stream.writeString(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBitSet(BitSet val) {
        DirectByteBufferStream stream = curStream;

        stream.writeBitSet(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeUuid(UUID val) {
        DirectByteBufferStream stream = curStream;

        stream.writeUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIgniteUuid(IgniteUuid val) {
        DirectByteBufferStream stream = curStream;

        stream.writeIgniteUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeAffinityTopologyVersion(AffinityTopologyVersion val) {
        DirectByteBufferStream stream = curStream;

        stream.writeAffinityTopologyVersion(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeMessage(@Nullable Message msg, boolean compress) {
        DirectByteBufferStream stream = curStream;

        if (compress)
            writeCompressedMessage(
                w -> w.state.item().stream.writeMessage(msg, w),
                msg == null,
                stream
            );
        else
            stream.writeMessage(msg, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeCacheObject(@Nullable CacheObject obj) {
        DirectByteBufferStream stream = curStream;

        stream.writeCacheObject(obj);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeKeyCacheObject(KeyCacheObject obj) {
        DirectByteBufferStream stream = curStream;

        stream.writeKeyCacheObject(obj);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeGridLongList(@Nullable GridLongList ll) {
        DirectByteBufferStream stream = curStream;

        stream.writeGridLongList(ll);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeObjectArray(T[] arr, MessageArrayType type) {
        DirectByteBufferStream stream = curStream;

        stream.writeObjectArray(arr, type, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeCollection(Collection<T> col, MessageCollectionType type) {
        DirectByteBufferStream stream = curStream;

        stream.writeCollection(col, type, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean writeMap(Map<K, V> map, MessageMapType type, boolean compress) {
        DirectByteBufferStream stream = curStream;

        if (compress)
            writeCompressedMessage(
                w -> w.state.item().stream.writeMap(map, type, w),
                map == null,
                stream
            );
        else
            stream.writeMap(map, type, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIgniteProductVersion(IgniteProductVersion ver) {
        DirectByteBufferStream stream = curStream;

        stream.writeIgniteProductVersion(ver);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeGridCacheVersion(GridCacheVersion ver) {
        DirectByteBufferStream stream = curStream;

        stream.writeGridCacheVersion(ver);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeaderWritten() {
        return state.item().hdrWritten;
    }

    /** {@inheritDoc} */
    @Override public void onHeaderWritten() {
        state.item().hdrWritten = true;
    }

    /** {@inheritDoc} */
    @Override public int state() {
        return state.item().state;
    }

    /** {@inheritDoc} */
    @Override public void incrementState() {
        state.item().state++;
    }

    /** {@inheritDoc} */
    @Override public void decrementState() {
        state.item().state--;
    }

    /** {@inheritDoc} */
    @Override public void beforeNestedWrite() {
        state.forward();

        curStream = state.item().stream;

        curStream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public void afterNestedWrite(boolean finished) {
        state.backward(finished);

        curStream = state.item().stream;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        state.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectMessageWriter.class, this);
    }

    /**
     * @param consumer Consumer.
     * @param isNull {@code True} if message is null.
     * @param stream Byte buffer stream.
     */
    private void writeCompressedMessage(Consumer<DirectMessageWriter> consumer, boolean isNull, DirectByteBufferStream stream) {
        if (isNull) {
            stream.writeShort(Short.MIN_VALUE);

            return;
        }

        if (!stream.serializeFinished()) {
            // CompressedMessage consumes the scratch buffer in its constructor (deflates into its own byte array),
            // so the buffer never escapes this method and can be reused across fields; heap is cheaper than direct
            // here (no native alloc / Cleaner churn).
            if (tmpBuf == null)
                tmpBuf = ByteBuffer.allocate(TMP_BUF_CAPACITY);
            else
                tmpBuf.clear();

            // Reuse the temp writer across fields/messages instead of allocating a fresh state stack each time.
            if (tmpWriter == null)
                tmpWriter = new DirectMessageWriter(msgFactory, compressionLvl);
            else
                tmpWriter.reset();

            tmpWriter.setBuffer(tmpBuf);

            boolean finished;

            do {
                if (tmpBuf.remaining() <= tmpBuf.capacity() / 10) {
                    ByteBuffer newBuf = ByteBuffer.allocate(tmpBuf.capacity() * 2);

                    tmpBuf.flip();
                    newBuf.put(tmpBuf);

                    tmpBuf = newBuf;

                    tmpWriter.setBuffer(tmpBuf);
                }

                consumer.accept(tmpWriter);

                finished = tmpWriter.state.item().stream.lastFinished();
            }
            while (!finished);

            tmpBuf.flip();

            stream.compressedMessage(new CompressedMessage(tmpBuf, compressionLvl));
            stream.serializeFinished(true);
        }

        stream.writeMessage(stream.compressedMessage(), this);

        if (stream.lastFinished()) {
            stream.compressedMessage(null);
            stream.serializeFinished(false);
        }
    }

    /**
     */
    private static class StateItem implements DirectMessageStateItem {
        /** */
        private final DirectByteBufferStream stream;

        /** */
        private int state;

        /** */
        private boolean hdrWritten;

        /** */
        public StateItem(MessageFactory msgFactory) {
            stream = new DirectByteBufferStream(msgFactory);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            state = 0;
            hdrWritten = false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StateItem.class, this);
        }
    }
}

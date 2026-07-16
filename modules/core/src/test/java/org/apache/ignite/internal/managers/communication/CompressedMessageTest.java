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

package org.apache.ignite.internal.managers.communication;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.zip.Deflater;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.direct.state.DirectMessageState;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPair;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link CompressedMessage}. */
public class CompressedMessageTest {
    /** */
    private static final MessageFactory MSG_FACTORY = new IgniteMessageFactoryImpl(new MessageFactoryProvider[]{
        new CoreMessagesProvider(jdk(), jdk(), U.gridClassLoader())});

    /** */
    @Test
    public void testWriteReadHugeMessage() {
        DirectMessageWriter writer = new DirectMessageWriter(MSG_FACTORY);

        ByteBuffer tmpBuf = ByteBuffer.allocate(4096);

        writer.setBuffer(tmpBuf);

        GridDhtPartitionsFullMessage fullMsg = fullMessage();

        boolean finished = false;
        boolean checkChunkCnt = true;

        int cnt = 0;

        ByteBuffer msgBuf = ByteBuffer.allocate(40_960);

        while (!finished) {
            finished = writer.writeMessage(fullMsg, true);

            if (checkChunkCnt) {
                DirectMessageState<?> state = U.field(writer, "state");
                DirectByteBufferStream stream = U.field(state.item(), "stream");

                CompressedMessage compressedMsg = stream.compressedMessage();

                assertTrue(compressedMsg.dataSize() > 0);

                List<byte[]> chunks = U.field(compressedMsg, "chunks");

                assertTrue(chunks.size() > 2);

                checkChunkCnt = false;
            }

            tmpBuf.flip();

            msgBuf.put(tmpBuf);

            tmpBuf.clear();

            cnt++;
        }

        assertTrue(cnt > 2);

        msgBuf.flip();

        DirectMessageReader reader = new DirectMessageReader(MSG_FACTORY, null);

        reader.setBuffer(msgBuf);

        Message readMsg = reader.readMessage(true);

        assertTrue(readMsg instanceof GridDhtPartitionsFullMessage);

        assertEqualsFullMsg(fullMsg, (GridDhtPartitionsFullMessage)readMsg);
    }

    /** Read must fail with an exception on a null chunk from the wire instead of looping forever. */
    @Test
    public void testReadFailsOnNullChunk() {
        DirectMessageWriter writer = new DirectMessageWriter(MSG_FACTORY);

        ByteBuffer buf = ByteBuffer.allocate(16);

        writer.setBuffer(buf);

        // Emulate a corrupted stream or an incompatible peer: dataSize > 0, non-final chunk, then the null-array
        // marker (-1), which CompressedMessageSerializer.writeTo() never produces at the chunk position.
        writer.writeInt(100);
        writer.writeBoolean(false);
        writer.writeByteArray(null);

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(MSG_FACTORY, null);

        reader.setBuffer(buf);

        GridTestUtils.assertThrows(null,
            () -> MessageSerialization.readFrom(MSG_FACTORY, new CompressedMessage(), reader),
            IgniteException.class,
            "unexpected null chunk");
    }

    /** Read must fail fast on a negative data size from the wire. */
    @Test
    public void testReadFailsOnNegativeDataSize() {
        DirectMessageWriter writer = new DirectMessageWriter(MSG_FACTORY);

        ByteBuffer buf = ByteBuffer.allocate(16);

        writer.setBuffer(buf);

        writer.writeInt(-5);

        buf.flip();

        DirectMessageReader reader = new DirectMessageReader(MSG_FACTORY, null);

        reader.setBuffer(buf);

        GridTestUtils.assertThrows(null,
            () -> MessageSerialization.readFrom(MSG_FACTORY, new CompressedMessage(), reader),
            IgniteException.class,
            "Invalid compressed message data size");
    }

    /** Uncompress must fail when dataSize > 0 but no chunks were received. */
    @Test
    public void testUncompressFailsWithoutChunks() {
        CompressedMessage rcvd = new CompressedMessage();

        rcvd.dataSize = 100;
        rcvd.finalChunk = true;

        GridTestUtils.assertThrows(null, rcvd::uncompressed, IgniteException.class, "truncated");
    }

    /** Uncompress must fail when the stream inflates to more bytes than the size header claims. */
    @Test
    public void testUncompressFailsOnUnderstatedDataSize() {
        byte[] data = new byte[1000];

        CompressedMessage sent = new CompressedMessage(ByteBuffer.wrap(data), Deflater.BEST_SPEED);

        CompressedMessage rcvd = new CompressedMessage();

        rcvd.dataSize = data.length - 1;
        rcvd.chunks = sent.chunks;
        rcvd.finalChunk = true;

        GridTestUtils.assertThrows(null, rcvd::uncompressed, IgniteException.class, "longer than expected");
    }

    /** Same as {@link #testUncompressFailsOnUnderstatedDataSize()}, but with a multi-chunk compressed stream. */
    @Test
    public void testUncompressFailsOnUnderstatedDataSizeMultiChunk() {
        byte[] data = new byte[CompressedMessage.CHUNK_SIZE * 3];

        new Random(42).nextBytes(data);

        CompressedMessage sent = new CompressedMessage(ByteBuffer.wrap(data), Deflater.BEST_SPEED);

        assertTrue(sent.chunks.size() > 1);

        CompressedMessage rcvd = new CompressedMessage();

        rcvd.dataSize = CompressedMessage.CHUNK_SIZE / 2;
        rcvd.chunks = sent.chunks;
        rcvd.finalChunk = true;

        GridTestUtils.assertThrows(null, rcvd::uncompressed, IgniteException.class, "longer than expected");
    }

    /** A complete envelope whose payload doesn't deserialize fully must fail instead of hanging as a partial read. */
    @Test
    public void testReadFailsOnTruncatedPayload() {
        DirectMessageWriter writer = new DirectMessageWriter(MSG_FACTORY);

        ByteBuffer tmpBuf = ByteBuffer.allocate(1 << 20);

        writer.setBuffer(tmpBuf);

        assertTrue(writer.writeMessage(fullMessage(), false));

        tmpBuf.flip();

        tmpBuf.limit(tmpBuf.limit() - 5); // Truncate the serialized message.

        CompressedMessage compressedMsg = new CompressedMessage(tmpBuf, Deflater.BEST_SPEED);

        DirectMessageWriter wireWriter = new DirectMessageWriter(MSG_FACTORY);

        ByteBuffer wire = ByteBuffer.allocate(1 << 20);

        wireWriter.setBuffer(wire);

        assertTrue(wireWriter.writeMessage(compressedMsg, false));

        wire.flip();

        DirectMessageReader reader = new DirectMessageReader(MSG_FACTORY, null);

        reader.setBuffer(wire);

        GridTestUtils.assertThrows(null, () -> reader.readMessage(true), IgniteException.class, "ended unexpectedly");
    }

    /** */
    private GridDhtPartitionsFullMessage fullMessage() {
        Map<UUID, Map<GroupPartitionIdPair, Long>> partHistSuppliers = new HashMap<>();
        Map<UUID, Map<Integer, Set<Integer>>> partsToReload = new HashMap<>();

        for (int i = 0; i < 500; i++) {
            UUID uuid = UUID.randomUUID();

            partHistSuppliers.put(uuid, Map.of(new GroupPartitionIdPair(i, i + 1), i + 2L));
            partsToReload.put(uuid, Map.of(i, Set.of(i + 1)));
        }

        return new GridDhtPartitionsFullMessage(null, null, new AffinityTopologyVersion(0), partHistSuppliers, partsToReload);
    }

    /** */
    private void assertEqualsFullMsg(GridDhtPartitionsFullMessage expected, GridDhtPartitionsFullMessage actual) {
        Map<UUID, Map<GroupPartitionIdPair, Long>> expHistSuppliers = expected.partitionHistorySuppliers();
        Map<UUID, Map<GroupPartitionIdPair, Long>> actHistSuppliers = actual.partitionHistorySuppliers();

        assertEquals(expHistSuppliers.size(), actHistSuppliers.size());

        for (Map.Entry<UUID, Map<GroupPartitionIdPair, Long>> entry : expHistSuppliers.entrySet())
            assertEquals(entry.getValue(), actHistSuppliers.get(entry.getKey()));

        Map<UUID, Map<Integer, Set<Integer>>> expPartsToReload = U.field(expected, "partsToReload");
        Map<UUID, Map<Integer, Set<Integer>>> actPartsToReload = U.field(actual, "partsToReload");

        assertEquals(expPartsToReload.size(), actPartsToReload.size());

        for (Map.Entry<UUID, Map<Integer, Set<Integer>>> entry : expPartsToReload.entrySet()) {
            Map<Integer, Set<Integer>> expCachePartitions = entry.getValue();
            Map<Integer, Set<Integer>> actCachePartitions = actPartsToReload.get(entry.getKey());

            assertEquals(expCachePartitions.size(), actCachePartitions.size());

            for (Map.Entry<Integer, Set<Integer>> partsEntry : expCachePartitions.entrySet())
                assertEquals(partsEntry.getValue(), actCachePartitions.get(partsEntry.getKey()));
        }
    }
}

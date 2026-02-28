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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.direct.state.DirectMessageState;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionReservationsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsToReload;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link CompressedMessage}. */
public class CompressedMessageTest {
    /** */
    @Test
    public void testWriteReadHugeMessage() {
        MessageFactory msgFactory = new IgniteMessageFactoryImpl(new MessageFactoryProvider[]{new GridIoMessageFactory()});

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

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

                byte[] compressedData = U.field((Object)U.field(compressedMsg, "chunkedReader"), "inputData");

                assertTrue(compressedData.length > CompressedMessage.CHUNK_SIZE * 2);

                checkChunkCnt = false;
            }

            tmpBuf.flip();

            msgBuf.put(tmpBuf);

            tmpBuf.clear();

            cnt++;
        }

        assertTrue(cnt > 2);

        msgBuf.flip();

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);

        reader.setBuffer(msgBuf);

        Message readMsg = reader.readMessage(true);

        assertTrue(readMsg instanceof GridDhtPartitionsFullMessage);

        assertEqualsFullMsg(fullMsg, (GridDhtPartitionsFullMessage)readMsg);
    }

    /** */
    private GridDhtPartitionsFullMessage fullMessage() {
        IgniteDhtPartitionHistorySuppliersMap partHistSuppliers = new IgniteDhtPartitionHistorySuppliersMap();
        IgniteDhtPartitionsToReloadMap partsToReload = new IgniteDhtPartitionsToReloadMap();

        for (int i = 0; i < 500; i++) {
            UUID uuid = UUID.randomUUID();

            partHistSuppliers.put(uuid, i, i + 1, i + 2);
            partsToReload.put(uuid, i, i + 1);
        }

        return new GridDhtPartitionsFullMessage(null, null, new AffinityTopologyVersion(0), partHistSuppliers, partsToReload);
    }

    /** */
    private void assertEqualsFullMsg(GridDhtPartitionsFullMessage expected, GridDhtPartitionsFullMessage actual) {
        Map<UUID, PartitionReservationsMap> expHistSuppliers = U.field(expected.partitionHistorySuppliers(), "map");
        Map<UUID, PartitionReservationsMap> actHistSuppliers = U.field(actual.partitionHistorySuppliers(), "map");

        assertEquals(expHistSuppliers.size(), actHistSuppliers.size());

        for (Map.Entry<UUID, PartitionReservationsMap> entry : expHistSuppliers.entrySet())
            assertEquals(entry.getValue().reservations(), actHistSuppliers.get(entry.getKey()).reservations());

        Map<UUID, CachePartitionsToReloadMap> expPartsToReload = U.field((Object)U.field(expected, "partsToReload"), "map");
        Map<UUID, CachePartitionsToReloadMap> actPartsToReload = U.field((Object)U.field(actual, "partsToReload"), "map");

        assertEquals(expPartsToReload.size(), actPartsToReload.size());

        for (Map.Entry<UUID, CachePartitionsToReloadMap> entry : expPartsToReload.entrySet()) {
            Map<Integer, PartitionsToReload> expCachePartitions = U.field(entry.getValue(), "map");
            Map<Integer, PartitionsToReload> actCachePartitions = U.field(actPartsToReload.get(entry.getKey()), "map");

            assertEquals(expCachePartitions.size(), actCachePartitions.size());

            for (Map.Entry<Integer, PartitionsToReload> partsEntry : expCachePartitions.entrySet())
                assertEquals(partsEntry.getValue().partitions(), actCachePartitions.get(partsEntry.getKey()).partitions());
        }
    }
}

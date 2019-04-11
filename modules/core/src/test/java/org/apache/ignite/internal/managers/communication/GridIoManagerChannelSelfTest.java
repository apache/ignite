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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 *
 */
public class GridIoManagerChannelSelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testCreateChannelToCustomRemoteTopic() throws Exception {
        startGrids(2);

        final IgniteSocketChannel[] nioCh = new IgniteSocketChannel[1];
        final CountDownLatch waitChLatch = new CountDownLatch(1);

        Object topic = TOPIC_CACHE.topic("channel", 0);

        grid(1).context().io().addChannelListener(topic, new GridIoChannelListener() {
            @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                // Created from ignite node with index = 0;
                if (channel.nodeId().equals(grid(0).localNode().id())) {
                    nioCh[0] = channel;

                    waitChLatch.countDown();
                }
            }
        });

        WritableByteChannel writableCh = grid(0).context()
            .io()
            .channelToTopic(grid(1).localNode().id(),
                topic,
                PUBLIC_POOL)
            .channel();

        // Wait for the channel connection established.
        assertTrue(waitChLatch.await(5_000L, TimeUnit.MILLISECONDS));

        assertNotNull(nioCh[0]);

        // Prepare ping bytes to check connection.
        final int pingNum = 777_777;
        final int pingBuffSize = 4;

        ByteBuffer writeBuf = ByteBuffer.allocate(pingBuffSize);

        writeBuf.putInt(pingNum);
        writeBuf.flip();

        // Write ping bytes to the channel.
        int cnt = writableCh.write(writeBuf);

        assertEquals(pingBuffSize, cnt);

        // Read test bytes from channel on remote node.
        ReadableByteChannel readCh = nioCh[0].channel();

        ByteBuffer readBuf = ByteBuffer.allocate(pingBuffSize);

        for (int i = 0; i < pingBuffSize; ) {
            int read = readCh.read(readBuf);

            if (read == -1)
                throw new IgniteException("Failed to read remote node ID");

            i += read;
        }

        readBuf.flip();

        // Check established channel.
        assertEquals(pingNum, readBuf.getInt());

        stopAllGrids();
    }
}

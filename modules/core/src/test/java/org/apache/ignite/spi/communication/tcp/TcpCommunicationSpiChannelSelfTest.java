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

package org.apache.ignite.spi.communication.tcp;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.util.nio.channel.GridNioSocketChannel;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TcpCommunicationSpiChannelSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TcpCommunicationSpi());
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testChannelCreationOnDemand() throws Exception {
        startGrids(NODES_CNT);

        final GridNioSocketChannel[] nioCh = new GridNioSocketChannel[1];
        final CountDownLatch waitChLatch = new CountDownLatch(1);

        grid(1).context().io().addChannelListener(new GridIoChannelListener() {
            @Override public void onChannelCreated(GridNioSocketChannel ch) {
                // Created from ignite node with index = 0;
                if (ch.id().nodeId().equals(grid(0).localNode().id())) {
                    nioCh[0] = ch;

                    waitChLatch.countDown();
                }
            }
        });

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        WritableByteChannel writableCh = commSpi.getOrCreateChannel(grid(1).localNode()).channel();

        // Wait for the channel connection established.
        waitChLatch.await(5_000L, TimeUnit.MILLISECONDS);

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
    }

}

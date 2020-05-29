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

package org.apache.ignite.internal;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class IgniteConnectionConcurrentReserveAndRemoveTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50 * 1024 * 1024));

        c.setDataStorageConfiguration(memCfg);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        spi.setIdleConnectionTimeout(Integer.MAX_VALUE);

        c.setCommunicationSpi(spi);

        return c;
    }

    /** */
    private static final class TestClosure implements IgniteCallable<Integer> {
        /** Serial version uid. */
        private static final long serialVersionUid = 0L;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return 1;
        }
    }

    @Test
    public void test() throws Exception {
        IgniteEx svr = startGrid(0);

        Ignite c1 = startClientGrid("client1");

        assertTrue(c1.configuration().isClientMode());

        Ignite c2 = startClientGrid("client2");

        assertTrue(c2.configuration().isClientMode());

        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi)c1.configuration().getCommunicationSpi();

        spi2.blockMessages(HandshakeMessage2.class, c1.name());

        AtomicInteger cnt = new AtomicInteger();

        cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));

        TcpCommunicationSpi spi1 = (TcpCommunicationSpi)c1.configuration().getCommunicationSpi();

        ConcurrentMap<UUID, GridCommunicationClient[]> clientsMap = U.field(spi1, "clients");

        GridCommunicationClient[] arr = clientsMap.get(c2.cluster().localNode().id());

        GridTcpNioCommunicationClient client = null;

        for (GridCommunicationClient c : arr) {
            client = (GridTcpNioCommunicationClient)c;

            if (client != null) {
                assertTrue(client.session().outRecoveryDescriptor().reserved());

                assertFalse(client.session().outRecoveryDescriptor().connected());
            }
        }

        assertNotNull(client);

        //spi1.failSend = true;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                doSleep(1000);

                //spi1.failSend = false;

                cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));
            }
        }, 1, "hang-thread");

        try {
            cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));

            //fail();
        }
        catch (IgniteException e) {
            // Expected.
        }

        fut.get();

        assertEquals(3, cnt.get());
    }
}

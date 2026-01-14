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

package org.apache.ignite.spi.discovery.tcp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests Java serialization header detection in discovery messages. */
public class TcpDiscoveryIoSessionDifferentSerializationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testDetectJavaObjectStreamHeader() throws Exception {
        IgniteEx grid = startGrid(0);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)grid.configuration().getDiscoverySpi();

        TcpDiscoveryHandshakeRequest req = new TcpDiscoveryHandshakeRequest();

        byte[] bytes = U.marshal(spi.marshaller(), req);

        TcpDiscoveryIoSession ses = new TcpDiscoveryIoSession(
            new TestSocket(new ByteArrayInputStream(bytes), new ByteArrayOutputStream()),
            spi
        );

        Throwable e = GridTestUtils.assertThrows(log, () -> ses.readMessage(), IgniteCheckedException.class, null);

        assertTrue(X.hasCause(e, "Incompatible discovery protocol: received Java ObjectStream header", IOException.class));
    }

    /** */
    private static class TestSocket extends Socket {
        /** */
        private final InputStream in;

        /** */
        private final OutputStream out;

        /**
         * @param in Input stream.
         * @param out Output stream.
         */
        private TestSocket(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        /** {@inheritDoc} */
        @Override public InputStream getInputStream() {
            return in;
        }

        /** {@inheritDoc} */
        @Override public OutputStream getOutputStream() {
            return out;
        }
    }
}

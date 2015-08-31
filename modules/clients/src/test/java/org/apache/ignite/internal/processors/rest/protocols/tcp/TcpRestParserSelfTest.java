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

package org.apache.ignite.internal.processors.rest.protocols.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.client.marshaller.optimized.GridClientOptimizedMarshaller;
import org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientHandshakeRequest;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.CAS;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.IGNITE_HANDSHAKE_FLAG;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.IGNITE_REQ_FLAG;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.MEMCACHE_REQ_FLAG;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MARSHALLER;

/**
 * This class tests that parser confirms memcache extended specification.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class TcpRestParserSelfTest extends GridCommonAbstractTest {
    /** Marshaller. */
    private GridClientMarshaller marshaller = new GridClientOptimizedMarshaller();

    /** Extras value. */
    public static final byte[] EXTRAS = new byte[]{
        (byte)0xDE, 0x00, (byte)0xBE, 0x00, //Flags, string encoding.
        0x00, 0x00, 0x00, 0x00 // Expiration value.
    };

    /**
     * @throws Exception If failed.
     */
    public void testSimplePacketParsing() throws Exception {
        GridNioSession ses = new MockNioSession();

        GridTcpRestParser parser = new GridTcpRestParser(false);

        byte hdr = MEMCACHE_REQ_FLAG;

        byte[] opCodes = {0x01, 0x02, 0x03};

        byte[] opaque = new byte[] {0x01, 0x02, 0x03, (byte)0xFF};

        String key = "key";

        String val = "value";

        for (byte opCode : opCodes) {
            ByteBuffer raw = rawPacket(hdr, opCode, opaque, key.getBytes(), val.getBytes(), EXTRAS);

            GridClientMessage msg = parser.decode(ses, raw);

            assertTrue(msg instanceof GridMemcachedMessage);

            GridMemcachedMessage packet = (GridMemcachedMessage)msg;

            assertEquals("Parser leaved unparsed bytes", 0, raw.remaining());

            assertEquals("Invalid opcode", opCode, packet.operationCode());
            assertEquals("Invalid key", key, packet.key());
            assertEquals("Invalid value", val, packet.value());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncorrectPackets() throws Exception {
        final GridNioSession ses = new MockNioSession();

        final GridTcpRestParser parser = new GridTcpRestParser(false);

        final byte[] opaque = new byte[] {0x01, 0x02, 0x03, (byte)0xFF};

        final String key = "key";

        final String val = "value";

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                parser.decode(ses, rawPacket((byte)0x01, (byte)0x01, opaque, key.getBytes(), val.getBytes(), EXTRAS));

                return null;
            }
        }, IOException.class, null);


        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                parser.decode(ses, rawPacket(MEMCACHE_REQ_FLAG, (byte)0x01, opaque, key.getBytes(), val.getBytes(), null));

                return null;
            }
        }, IOException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                ByteBuffer fake = ByteBuffer.allocate(21);

                fake.put(IGNITE_REQ_FLAG);
                fake.put(U.intToBytes(-5));
                fake.put(U.longToBytes(0));
                fake.put(U.longToBytes(0));

                fake.flip();

                parser.decode(ses, fake);

                return null;
            }
        }, IOException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomMessages() throws Exception {
        GridClientCacheRequest req = new GridClientCacheRequest(CAS);

        req.key("key");
        req.value(1);
        req.value2(2);
        req.clientId(UUID.randomUUID());

        ByteBuffer raw = clientRequestPacket(req);

        GridNioSession ses = new MockNioSession();

        ses.addMeta(MARSHALLER.ordinal(), new GridClientOptimizedMarshaller());

        GridTcpRestParser parser = new GridTcpRestParser(false);

        GridClientMessage msg = parser.decode(ses, raw);

        assertNotNull(msg);

        assertEquals("Parser leaved unparsed bytes", 0, raw.remaining());

        assertTrue(msg instanceof GridClientCacheRequest);

        GridClientCacheRequest res = (GridClientCacheRequest) msg;

        assertEquals("Invalid operation", req.operation(), res.operation());
        assertEquals("Invalid clientId", req.clientId(), res.clientId());
        assertEquals("Invalid key", req.key(), res.key());
        assertEquals("Invalid value 1", req.value(), res.value());
        assertEquals("Invalid value 2", req.value2(), res.value2());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMixedParsing() throws Exception {
        GridNioSession ses1 = new MockNioSession();
        GridNioSession ses2 = new MockNioSession();

        ses1.addMeta(MARSHALLER.ordinal(), new GridClientOptimizedMarshaller());
        ses2.addMeta(MARSHALLER.ordinal(), new GridClientOptimizedMarshaller());

        GridTcpRestParser parser = new GridTcpRestParser(false);

        GridClientCacheRequest req = new GridClientCacheRequest(CAS);

        req.key("key");

        String val = "value";

        req.value(val);
        req.value2(val);
        req.clientId(UUID.randomUUID());

        byte[] opaque = new byte[]{0x01, 0x02, 0x03, (byte)0xFF};

        String key = "key";

        ByteBuffer raw1 = rawPacket(MEMCACHE_REQ_FLAG, (byte)0x01, opaque, key.getBytes(), val.getBytes(), EXTRAS);

        ByteBuffer raw2 = clientRequestPacket(req);

        raw1.mark();

        raw2.mark();

        int splits = Math.min(raw1.remaining(), raw2.remaining());

        for (int i = 1; i < splits; i++) {
            ByteBuffer[] packet1 = split(raw1, i);

            ByteBuffer[] packet2 = split(raw2, i);

            GridClientMessage msg = parser.decode(ses1, packet1[0]);

            assertNull(msg);

            msg = parser.decode(ses2, packet2[0]);

            assertNull(msg);

            msg = parser.decode(ses1, packet1[1]);

            assertTrue(msg instanceof GridMemcachedMessage);

            assertEquals(key, ((GridMemcachedMessage)msg).key());
            assertEquals(val, ((GridMemcachedMessage)msg).value());

            msg = parser.decode(ses2, packet2[1]);

            assertTrue(msg instanceof GridClientCacheRequest);

            assertEquals(val, ((GridClientCacheRequest)msg).value());
            assertEquals(val, ((GridClientCacheRequest)msg).value2());

            raw1.reset();

            raw2.reset();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testParseContinuousSplit() throws Exception {
        ByteBuffer tmp = ByteBuffer.allocate(10 * 1024);

        GridClientCacheRequest req = new GridClientCacheRequest(CAS);

        req.key("key");
        req.value(1);
        req.value2(2);
        req.clientId(UUID.randomUUID());

        for (int i = 0; i < 5; i++)
            tmp.put(clientRequestPacket(req));

        tmp.flip();

        for (int splitPos = 0; splitPos < tmp.remaining(); splitPos++) {
            ByteBuffer[] split = split(tmp, splitPos);

            tmp.flip();

            GridNioSession ses = new MockNioSession();

            ses.addMeta(MARSHALLER.ordinal(), new GridClientOptimizedMarshaller());

            GridTcpRestParser parser = new GridTcpRestParser(false);

            Collection<GridClientCacheRequest> lst = new ArrayList<>(5);

            for (ByteBuffer buf : split) {
                GridClientCacheRequest r;

                while (buf.hasRemaining() && (r = (GridClientCacheRequest)parser.decode(ses, buf)) != null)
                    lst.add(r);

                assertTrue("Parser has left unparsed bytes.", buf.remaining() == 0);
            }

            assertEquals(5, lst.size());

            for (GridClientCacheRequest res : lst) {
                assertEquals("Invalid operation", req.operation(), res.operation());
                assertEquals("Invalid clientId", req.clientId(), res.clientId());
                assertEquals("Invalid key", req.key(), res.key());
                assertEquals("Invalid value 1", req.value(), res.value());
                assertEquals("Invalid value 2", req.value2(), res.value2());
            }
        }
    }

    /**
     * Tests correct parsing of client handshake packets.
     *
     * @throws Exception If failed.
     */
    public void testParseClientHandshake() throws Exception {
        for (int splitPos = 1; splitPos < 5; splitPos++) {
            log.info("Checking split position: " + splitPos);

            ByteBuffer tmp = clientHandshakePacket();

            ByteBuffer[] split = split(tmp, splitPos);

            GridNioSession ses = new MockNioSession();

            ses.addMeta(MARSHALLER.ordinal(), new GridClientOptimizedMarshaller());

            GridTcpRestParser parser = new GridTcpRestParser(false);

            Collection<GridClientMessage> lst = new ArrayList<>(1);

            for (ByteBuffer buf : split) {
                GridClientMessage r;

                while (buf.hasRemaining() && (r = parser.decode(ses, buf)) != null)
                    lst.add(r);

                assertTrue("Parser has left unparsed bytes.", buf.remaining() == 0);
            }

            assertEquals(1, lst.size());

            GridClientHandshakeRequest req = (GridClientHandshakeRequest)F.first(lst);

            assertNotNull(req);
            assertEquals(U.bytesToShort(new byte[]{5, 0}, 0), req.version());
        }
    }

    /**
     * Splits given byte buffer into two byte buffers.
     *
     * @param original Original byte buffer.
     * @param pos Position at which buffer should be split.
     * @return Array of byte buffers.
     */
    private ByteBuffer[] split(ByteBuffer original, int pos) {

        byte[] data = new byte[pos];

        original.get(data);

        ByteBuffer[] res = new ByteBuffer[2];

        res[0] = ByteBuffer.wrap(data);

        data = new byte[original.remaining()];

        original.get(data);

        res[1] = ByteBuffer.wrap(data);

        return res;
    }

    /**
     * Assembles Ignite client packet.
     *
     * @param msg Message to serialize.
     * @return Raw message bytes.
     * @throws IOException If serialization failed.
     */
    private ByteBuffer clientRequestPacket(GridClientMessage msg) throws IOException {
        ByteBuffer res = marshaller.marshal(msg, 45);

        ByteBuffer slice = res.slice();

        slice.put(IGNITE_REQ_FLAG);
        slice.putInt(res.remaining() - 5);
        slice.putLong(msg.requestId());
        slice.put(U.uuidToBytes(msg.clientId()));
        slice.put(U.uuidToBytes(msg.destinationId()));

        return res;
    }

    /**
     * Assembles Ignite client handshake packet.
     *
     * @return Raw message bytes.
     */
    private ByteBuffer clientHandshakePacket() {
        ByteBuffer res = ByteBuffer.allocate(6);

        res.put(new byte[] {
            IGNITE_HANDSHAKE_FLAG, 5, 0, 0, 0, 0
        });

        res.flip();

        return res;
    }

    /**
     * Assembles raw packet without any logical checks.
     *
     * @param magic Header for packet.
     * @param opCode Operation code.
     * @param opaque Opaque value.
     * @param key Key data.
     * @param val Value data.
     * @param extras Extras data.
     * @return Byte buffer containing assembled packet.
     */
    private ByteBuffer rawPacket(byte magic, byte opCode, byte[] opaque, @Nullable byte[] key, @Nullable byte[] val,
        @Nullable byte[] extras) {
        // 1k should be enough.
        ByteBuffer res = ByteBuffer.allocate(1024);

        res.put(magic);
        res.put(opCode);

        int keyLen = key == null ? 0 : key.length;
        int extrasLen = extras == null ? 0 : extras.length;
        int valLen = val == null ? 0 : val.length;

        res.putShort((short)keyLen);
        res.put((byte)extrasLen);

        // Data type is always 0.
        res.put((byte)0);

        // Reserved.
        res.putShort((short)0);

        // Total body.
        res.putInt(keyLen + extrasLen + valLen);

        // Opaque.
        res.put(opaque);

        // CAS
        res.putLong(0);

        if (extrasLen > 0)
            res.put(extras);

        if (keyLen > 0)
            res.put(key);

        if (valLen > 0)
            res.put(val);

        res.flip();

        return res;
    }
}
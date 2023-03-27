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

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test Redis request parser.
 * @see <a href="https://redis.io/docs/reference/protocol-spec/">Redis protocol specification.</a>
 */
public class TcpRedisParserSelfTest {
    /** Test check parser works with the message splitted to several network packets. */
    @Test
    public void testSplitedPacketParse() throws Exception {
        String cmd = "SET";
        String var = "var";
        String data = RandomStringUtils.randomAscii(10);

        String fullMsg = "*3\r\n" +
            "$" + cmd.length() + "\r\n" + cmd + "\r\n" +
            "$" + var.length() + "\r\n" + var + "\r\n" +
            "$" + data.length() + "\r\n" + data + "\r\n";

        Consumer<GridClientMessage> checker = m -> {
            GridRedisMessage msg = (GridRedisMessage)m;

            assertNotNull(msg);

            assertEquals(3, msg.fullLength());
            assertEquals(3, msg.messageSize());
            assertEquals(cmd, msg.aux(0));
            assertEquals(var, msg.aux(1));
            assertEquals(data, msg.aux(2));
        };

        for (int i = 1; i < fullMsg.length() - 1; i++) {
            String packet0 = fullMsg.substring(0, i);
            String packet1 = fullMsg.substring(i);

            GridTcpRestParser.ParserState state = new GridTcpRestParser.ParserState();

            state.packetType(GridClientPacketType.REDIS);

            assertNull(GridTcpRestParser.parseRedisPacket(ByteBuffer.wrap(packet0.getBytes()), state));

            checker.accept(GridTcpRestParser.parseRedisPacket(ByteBuffer.wrap(packet1.getBytes()), state));
        }

        for (int i = 1; i < fullMsg.length() - 2; i++) {
            for (int j = i; j < fullMsg.length() - 1; j++) {
                String packet0 = fullMsg.substring(0, i);
                String packet1 = fullMsg.substring(i, j);
                String packet2 = fullMsg.substring(j);

                GridTcpRestParser.ParserState state = new GridTcpRestParser.ParserState();

                state.packetType(GridClientPacketType.REDIS);

                assertNull(GridTcpRestParser.parseRedisPacket(ByteBuffer.wrap(packet0.getBytes()), state));
                assertNull(GridTcpRestParser.parseRedisPacket(ByteBuffer.wrap(packet1.getBytes()), state));
                checker.accept(GridTcpRestParser.parseRedisPacket(ByteBuffer.wrap(packet2.getBytes()), state));
            }
        }
    }
}

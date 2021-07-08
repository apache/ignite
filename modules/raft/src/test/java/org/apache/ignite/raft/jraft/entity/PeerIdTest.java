/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.entity;

import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PeerIdTest {

    @Test
    public void testToStringParse() {
        final PeerId peer = new PeerId("192.168.1.1", 8081, 0);
        assertEquals("192.168.1.1:8081", peer.toString());

        final PeerId pp = new PeerId();
        assertTrue(pp.parse(peer.toString()));
        assertEquals(8081, pp.getPort());
        assertEquals("192.168.1.1", pp.getIp());
        assertEquals(0, pp.getIdx());
        assertEquals(pp, peer);
        assertEquals(pp.hashCode(), peer.hashCode());
    }

    @Test
    public void testIsPriorityNotElected() {

        final Endpoint endpoint1 = new Endpoint("192.168.1.1", 8081);
        final PeerId peer1 = new PeerId(endpoint1, 0, 0);
        assertEquals("192.168.1.1:8081::0", peer1.toString());
        assertTrue(peer1.isPriorityNotElected());
    }

    @Test
    public void testIsPriorityDisabled() {

        final Endpoint endpoint1 = new Endpoint("192.168.1.1", 8081);
        final PeerId peer1 = new PeerId(endpoint1, 0);
        assertEquals("192.168.1.1:8081", peer1.toString());
        assertTrue(peer1.isPriorityDisabled());
    }

    @Test
    public void testToStringParseWithIdxAndPriority() {

        // 1.String format is, ip:port::priority
        final Endpoint endpoint1 = new Endpoint("192.168.1.1", 8081);
        final PeerId peer1 = new PeerId(endpoint1, 0, 100);
        assertEquals("192.168.1.1:8081::100", peer1.toString());

        final PeerId p1 = new PeerId();
        final String str1 = "192.168.1.1:8081::100";
        assertTrue(p1.parse(str1));
        assertEquals(8081, p1.getPort());
        assertEquals("192.168.1.1", p1.getIp());
        assertEquals(0, p1.getIdx());
        assertEquals(100, p1.getPriority());

        assertEquals(p1, peer1);
        assertEquals(p1.hashCode(), peer1.hashCode());

        // 2.String format is, ip:port:idx:priority
        final Endpoint endpoint2 = new Endpoint("192.168.1.1", 8081);
        final PeerId peer2 = new PeerId(endpoint2, 100, 200);
        assertEquals("192.168.1.1:8081:100:200", peer2.toString());

        final PeerId p2 = new PeerId();
        final String str2 = "192.168.1.1:8081:100:200";
        assertTrue(p2.parse(str2));
        assertEquals(8081, p2.getPort());
        assertEquals("192.168.1.1", p2.getIp());
        assertEquals(100, p2.getIdx());
        assertEquals(200, p2.getPriority());

        assertEquals(p2, peer2);
        assertEquals(p2.hashCode(), peer2.hashCode());
    }

    @Test
    public void testIdx() {
        final PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        assertEquals("192.168.1.1:8081:1", peer.toString());
        assertFalse(peer.isEmpty());

        final PeerId pp = new PeerId();
        assertTrue(pp.parse(peer.toString()));
        assertEquals(8081, pp.getPort());
        assertEquals("192.168.1.1", pp.getIp());
        assertEquals(1, pp.getIdx());
        assertEquals(pp, peer);
        assertEquals(pp.hashCode(), peer.hashCode());
    }

    @Test
    public void testParseFail() {
        final PeerId peer = new PeerId();
        assertTrue(peer.isEmpty());
        assertFalse(peer.parse("localhsot:2:3:4:5"));
        assertTrue(peer.isEmpty());
    }

    @Test
    public void testEmptyPeer() {
        PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        assertFalse(peer.isEmpty());
        peer = PeerId.emptyPeer();
        assertTrue(peer.isEmpty());
    }

    @Test
    public void testChecksum() {
        PeerId peer = new PeerId("192.168.1.1", 8081, 1);
        long c = peer.checksum();
        assertTrue(c != 0);
        assertEquals(c, peer.checksum());
    }

    @Test
    public void testToStringParseFailed() {
        final PeerId pp = new PeerId();
        final String str1 = "";
        final String str2 = "192.168.1.1";
        final String str3 = "92.168.1.1:8081::1:2";
        assertFalse(pp.parse(str1));
        assertFalse(pp.parse(str2));
    }
}

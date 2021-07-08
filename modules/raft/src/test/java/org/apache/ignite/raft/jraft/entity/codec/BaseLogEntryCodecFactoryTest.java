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
package org.apache.ignite.raft.jraft.entity.codec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class BaseLogEntryCodecFactoryTest {

    protected LogEntryEncoder encoder;
    protected LogEntryDecoder decoder;

    @BeforeEach
    public void setup() {
        LogEntryCodecFactory factory = newFactory();
        this.encoder = factory.encoder();
        this.decoder = factory.decoder();
    }

    protected abstract LogEntryCodecFactory newFactory();

    @Test
    public void testEncodeDecodeEmpty() {
        try {
            assertNull(this.encoder.encode(null));
            fail();
        }
        catch (NullPointerException e) {
            assertTrue(true);
        }
        assertNull(this.decoder.decode(null));
        assertNull(this.decoder.decode(new byte[0]));
    }

    @Test
    public void testEncodeDecodeWithoutData() {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        assertNull(entry.getData());
        assertNull(entry.getOldPeers());

        byte[] content = this.encoder.encode(entry);

        assertNotNull(content);
        assertTrue(content.length > 0);

        LogEntry nentry = this.decoder.decode(content);
        assertNotNull(nentry);

        assertEquals(100, nentry.getId().getIndex());
        assertEquals(3, nentry.getId().getTerm());
        assertEquals(EnumOutter.EntryType.ENTRY_TYPE_NO_OP, nentry.getType());
        assertEquals(2, nentry.getPeers().size());
        assertEquals("localhost:99:1", nentry.getPeers().get(0).toString());
        assertEquals("localhost:100:2", nentry.getPeers().get(1).toString());
        assertNull(nentry.getData());
        assertNull(nentry.getOldPeers());
    }

    @Test
    public void testEncodeDecodeWithData() {
        ByteBuffer buf = ByteBuffer.wrap("hello".getBytes());
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(100, 3));
        entry.setChecksum(123456L);
        entry.setData(buf);
        entry.setPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        entry.setOldPeers(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        entry.setLearners(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        entry.setOldLearners(Arrays.asList(new PeerId("localhost", 99, 1), new PeerId("localhost", 100, 2)));
        assertEquals(buf, entry.getData());

        byte[] content = this.encoder.encode(entry);

        assertNotNull(content);
        assertTrue(content.length > 0);

        LogEntry nentry = this.decoder.decode(content);
        assertNotNull(nentry);

        assertEquals(100, nentry.getId().getIndex());
        assertEquals(3, nentry.getId().getTerm());
        assertEquals(123456L, nentry.getChecksum());

        List<PeerId> peers = nentry.getPeers();

        assertEquals(2, peers.size());
        assertEquals("localhost:99:1", peers.get(0).toString());
        assertEquals("localhost:100:2", peers.get(1).toString());

        peers = nentry.getOldPeers();

        assertEquals(2, peers.size());
        assertEquals("localhost:99:1", peers.get(0).toString());
        assertEquals("localhost:100:2", peers.get(1).toString());

        peers = nentry.getLearners();

        assertEquals(2, peers.size());
        assertEquals("localhost:99:1", peers.get(0).toString());
        assertEquals("localhost:100:2", peers.get(1).toString());

        peers = nentry.getOldLearners();

        assertEquals(2, peers.size());
        assertEquals("localhost:99:1", peers.get(0).toString());
        assertEquals("localhost:100:2", peers.get(1).toString());

        assertEquals(buf, nentry.getData());
        assertEquals(0, nentry.getData().position());
        assertEquals(5, nentry.getData().remaining());
    }

}

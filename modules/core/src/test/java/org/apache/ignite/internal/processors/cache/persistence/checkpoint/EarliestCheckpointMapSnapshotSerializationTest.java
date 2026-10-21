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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that {@link EarliestCheckpointMapSnapshot} reads every layout of {@code cpMapSnapshot.bin} that was ever
 * written: the legacy one (2.17, 2.18, current) and the one of {@link EarliestCheckpointMapSnapshotSerializer}.
 */
public class EarliestCheckpointMapSnapshotSerializationTest extends GridCommonAbstractTest {
    /** Header written by 2.18 and later. */
    private static final int HDR = 0x42BEEF00;

    /** Header written by 2.17 and earlier: magic plus protocol version 1. */
    private static final int HDR_V1 = 0x42BEEF01;

    /** */
    private static final UUID CP1 = UUID.randomUUID();

    /** */
    private static final UUID CP2 = UUID.randomUUID();

    /** */
    private static final UUID CP_ABSENT = UUID.randomUUID();

    /** */
    @Test
    public void testCurrentWrites() throws Exception {
        check(serdes(false));
    }

    /** The upgrade case. */
    @Test
    public void testGeneratedWrites() throws Exception {
        check(serdes(true));
    }

    /** */
    @Test
    public void testEmptySnapshot() throws Exception {
        EarliestCheckpointMapSnapshot snap = read(write(new EarliestCheckpointMapSnapshot(), false));

        assertNull(snap.groupState(CP1));
        assertFalse(snap.checkpointWasPresent(CP1));
    }

    /** 2.17 wrote the protocol version in the header. */
    @Test
    public void testLegacyHeaderWithProtocolVersion() throws Exception {
        EarliestCheckpointMapSnapshot snap = snapshot();

        byte[] bytes = writeWithHeader(HDR_V1, payload -> {
            U.writeMap(payload, snap.data);
            U.writeCollection(payload, snap.checkpointIds);
        });

        check(read(bytes));
    }

    /**
     * Writes the snapshot with the generated serializer or the current code and reads it with the current code.
     *
     * @param writeGenerated {@code True} to write with {@link EarliestCheckpointMapSnapshotSerializer},
     *      {@code false} to write with {@link EarliestCheckpointMapSnapshot#writeExternal}.
     * @return Deserialized snapshot.
     */
    private static EarliestCheckpointMapSnapshot serdes(boolean writeGenerated) throws Exception {
        return read(write(snapshot(), writeGenerated));
    }

    /** */
    private static byte[] write(EarliestCheckpointMapSnapshot snap, boolean generated) throws IOException {
        if (generated)
            return writeWithHeader(HDR, payload -> new EarliestCheckpointMapSnapshotSerializer().writeExternal(snap, payload));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            snap.writeExternal(out);
        }

        return bos.toByteArray();
    }

    /** Reads with {@link EarliestCheckpointMapSnapshot#readExternal}. */
    private static EarliestCheckpointMapSnapshot read(byte[] bytes) throws Exception {
        EarliestCheckpointMapSnapshot snap = new EarliestCheckpointMapSnapshot();

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            snap.readExternal(in);
        }

        return snap;
    }

    /**
     * Writes the way {@code IgniteDataTransferObject#writeExternal} did: header, then the length-prefixed payload
     * produced by a nested {@link ObjectOutputStream}.
     */
    private static byte[] writeWithHeader(int hdr, PayloadWriter payloadWriter) throws IOException {
        ByteArrayOutputStream payloadBos = new ByteArrayOutputStream();

        try (ObjectOutputStream payload = new ObjectOutputStream(payloadBos)) {
            payloadWriter.write(payload);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeInt(hdr);

            U.writeByteArray(out, payloadBos.toByteArray());
        }

        return bos.toByteArray();
    }

    /** @return Snapshot with two checkpoints and several groups. */
    private static EarliestCheckpointMapSnapshot snapshot() {
        Map<UUID, Map<Integer, GroupStateSnapshot>> data = new HashMap<>();

        Map<Integer, GroupStateSnapshot> grps1 = new HashMap<>();

        grps1.put(1, new GroupStateSnapshot(new int[] {0, 1, 2}, new long[] {10, 11, 12}, 3));
        grps1.put(2, new GroupStateSnapshot(new int[] {5}, new long[] {50}, 1));

        Map<Integer, GroupStateSnapshot> grps2 = new HashMap<>();

        grps2.put(1, new GroupStateSnapshot(new int[] {0, 1}, new long[] {20, 21}, 2));

        data.put(CP1, grps1);
        data.put(CP2, grps2);

        Set<UUID> ids = new HashSet<>();

        ids.add(CP1);
        ids.add(CP2);

        return new EarliestCheckpointMapSnapshot(ids, data);
    }

    /** */
    private static void check(EarliestCheckpointMapSnapshot snap) {
        assertTrue(snap.checkpointWasPresent(CP1));
        assertTrue(snap.checkpointWasPresent(CP2));
        assertFalse(snap.checkpointWasPresent(CP_ABSENT));

        Map<Integer, CheckpointEntry.GroupState> grps1 = snap.groupState(CP1);

        assertEquals(2, grps1.size());

        assertEquals(3, grps1.get(1).size());
        assertEquals(10, grps1.get(1).counterByPartition(0));
        assertEquals(11, grps1.get(1).counterByPartition(1));
        assertEquals(12, grps1.get(1).counterByPartition(2));

        assertEquals(1, grps1.get(2).size());
        assertEquals(50, grps1.get(2).counterByPartition(5));

        Map<Integer, CheckpointEntry.GroupState> grps2 = snap.groupState(CP2);

        assertEquals(1, grps2.size());
        assertEquals(2, grps2.get(1).size());
        assertEquals(21, grps2.get(1).counterByPartition(1));

        assertNull(snap.groupState(CP_ABSENT));
    }

    /** */
    @FunctionalInterface
    private interface PayloadWriter {
        /** */
        void write(ObjectOutput out) throws IOException;
    }
}

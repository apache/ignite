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
import java.io.ObjectInput;
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
    private final EarliestCheckpointMapSnapshotSerializer generated = new EarliestCheckpointMapSnapshotSerializer();

    /** Current code writes, current code reads. */
    @Test
    public void testCurrentRoundTrip() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(snapshot());
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            check((EarliestCheckpointMapSnapshot)ois.readObject());
        }
    }

    /** Current code writes an empty snapshot, current code reads. */
    @Test
    public void testEmptySnapshot() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(new EarliestCheckpointMapSnapshot());
        }

        EarliestCheckpointMapSnapshot snap;

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            snap = (EarliestCheckpointMapSnapshot)ois.readObject();
        }

        assertNull(snap.groupState(CP1));
        assertFalse(snap.checkpointWasPresent(CP1));
    }

    /** 2.17 wrote the protocol version in the header, current code reads. */
    @Test
    public void testLegacyHeaderWithProtocolVersion() throws Exception {
        byte[] bytes = write(HDR_V1, out -> {
            U.writeMap(out, snapshot().data);
            U.writeCollection(out, snapshot().checkpointIds);
        });

        check(readCurrent(bytes));
    }

    /** Generated serializer writes, current code reads. */
    @Test
    public void testGeneratedWritesCurrentReads() throws Exception {
        check(readCurrent(write(HDR, out -> generated.writeExternal(snapshot(), out))));
    }

    /** Generated serializer writes, generated serializer reads. */
    @Test
    public void testGeneratedRoundTrip() throws Exception {
        byte[] bytes = write(HDR, out -> generated.writeExternal(snapshot(), out));

        EarliestCheckpointMapSnapshot snap = new EarliestCheckpointMapSnapshot();

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            assertEquals(HDR, in.readInt());

            try (ObjectInputStream payload = new ObjectInputStream(new ByteArrayInputStream(U.readByteArray(in)))) {
                generated.readExternal(snap, payload);
            }
        }

        check(snap);
    }

    /** Current code writes, generated serializer can't read: the layouts differ, that is why the fallback exists. */
    @Test
    public void testCurrentWritesGeneratedFails() throws Exception {
        byte[] bytes = write(HDR, out -> {
            U.writeMap(out, snapshot().data);
            U.writeCollection(out, snapshot().checkpointIds);
        });

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            in.readInt();

            try (ObjectInputStream payload = new ObjectInputStream(new ByteArrayInputStream(U.readByteArray(in)))) {
                generated.readExternal(new EarliestCheckpointMapSnapshot(), payload);

                fail("Generated serializer must not read the legacy layout");
            }
            catch (IOException ignored) {
                // Expected.
            }
        }
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

    /**
     * Writes the snapshot the way {@code IgniteDataTransferObject#writeExternal} did: header, then the length-prefixed
     * payload produced by a nested {@link ObjectOutputStream}.
     *
     * @param hdr Header.
     * @param payloadWriter Writes the payload.
     * @return Bytes to be read by {@link EarliestCheckpointMapSnapshot#readExternal}.
     */
    private static byte[] write(int hdr, PayloadWriter payloadWriter) throws IOException {
        ByteArrayOutputStream payloadBos = new ByteArrayOutputStream();

        try (ObjectOutputStream payload = new ObjectOutputStream(payloadBos)) {
            payloadWriter.write(payload);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeInt(hdr);

            U.writeByteArray(out, payloadBos.toByteArray());
        }

        return bos.toByteArray();
    }

    /** Reads bytes with {@link EarliestCheckpointMapSnapshot#readExternal}. */
    private static EarliestCheckpointMapSnapshot readCurrent(byte[] bytes) throws Exception {
        EarliestCheckpointMapSnapshot snap = new EarliestCheckpointMapSnapshot();

        try (ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            snap.readExternal(in);
        }

        return snap;
    }

    /** */
    @FunctionalInterface
    private interface PayloadWriter {
        /** */
        void write(ObjectOutput out) throws IOException;
    }
}

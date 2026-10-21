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

import java.io.IOException;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObjectOutput;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.GroupStateSnapshot;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.EarliestCheckpointMapSnapshot.MAGIC;
import static org.junit.Assert.assertArrayEquals;

/**
 * Checks that {@link EarliestCheckpointMapSnapshot} reads {@code cpMapSnapshot.bin} written by the current code and
 * by {@link EarliestCheckpointMapSnapshotSerializer}.
 */
public class EarliestCheckpointMapSnapshotSerializationTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testSerdes() throws Exception {
        EarliestCheckpointMapSnapshot empty = new EarliestCheckpointMapSnapshot();

        check(empty, serdes(empty, false));
        check(empty, serdes(empty, true));

        EarliestCheckpointMapSnapshot snap = snapshot();

        check(snap, serdes(snap, false));
        check(snap, serdes(snap, true));

    }

    /**
     * Marshals the snapshot with the generated serializer or the current code and unmarshals it the same way
     * {@code CheckpointMarkersStorage} does.
     *
     * @param snap Snapshot.
     * @param writeGenerated {@code True} to write with {@link EarliestCheckpointMapSnapshotSerializer},
     *      {@code false} to write with {@link EarliestCheckpointMapSnapshot#writeExternal}.
     * @return Deserialized snapshot.
     */
    private static EarliestCheckpointMapSnapshot serdes(
        EarliestCheckpointMapSnapshot snap,
        boolean writeGenerated
    ) throws Exception {
        JdkMarshaller marsh = Marshallers.jdk();

        byte[] bytes = marsh.marshal(writeGenerated ? new GeneratedLayoutSnapshot(snap) : snap);

        return marsh.unmarshal(bytes, null);
    }

    /** Written the way builds with the generated serializer did, read by the inherited current code. */
    private static class GeneratedLayoutSnapshot extends EarliestCheckpointMapSnapshot {
        /** Serial version UUID. */
        private static final long serialVersionUID = 0L;

        /** Constructor for serialization. */
        public GeneratedLayoutSnapshot() {
            // No-op.
        }

        /** @param snap Snapshot to copy. */
        GeneratedLayoutSnapshot(EarliestCheckpointMapSnapshot snap) {
            super(snap.checkpointIds, snap.data);
        }

        /** Same as {@code IgniteDataTransferObject#writeExternal}: header, then the payload of the generated serializer. */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(MAGIC);

            try (IgniteDataTransferObjectOutput dtout = new IgniteDataTransferObjectOutput(out)) {
                new EarliestCheckpointMapSnapshotSerializer().writeExternal(this, dtout);
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

        UUID cp1 = UUID.randomUUID();
        UUID cp2 = UUID.randomUUID();

        data.put(cp1, grps1);
        data.put(cp2, grps2);

        Set<UUID> ids = new HashSet<>();

        ids.add(cp1);
        ids.add(cp2);

        return new EarliestCheckpointMapSnapshot(ids, data);
    }

    /** Compares the snapshots field by field. */
    private static void check(EarliestCheckpointMapSnapshot exp, EarliestCheckpointMapSnapshot act) {
        assertEquals(exp.checkpointIds, act.checkpointIds);

        assertEquals(exp.data.keySet(), act.data.keySet());

        for (Map.Entry<UUID, Map<Integer, GroupStateSnapshot>> e : exp.data.entrySet()) {
            Map<Integer, GroupStateSnapshot> expGrps = e.getValue();
            Map<Integer, GroupStateSnapshot> actGrps = act.data.get(e.getKey());

            assertEquals(expGrps.keySet(), actGrps.keySet());

            for (Map.Entry<Integer, GroupStateSnapshot> g : expGrps.entrySet()) {
                GroupStateSnapshot expGrp = g.getValue();
                GroupStateSnapshot actGrp = actGrps.get(g.getKey());

                assertArrayEquals(expGrp.partitionIds(), actGrp.partitionIds());
                assertArrayEquals(expGrp.partitionCounters(), actGrp.partitionCounters());
                assertEquals(expGrp.size(), actGrp.size());
            }
        }
    }
}

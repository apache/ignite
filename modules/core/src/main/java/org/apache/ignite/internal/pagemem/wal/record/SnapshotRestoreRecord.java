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

package org.apache.ignite.internal.pagemem.wal.record;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SNAPSHOT_RESTORE_RECORD;

/**
 *
 */
public class SnapshotRestoreRecord extends WALRecord {
    /** Restoring snapshot name. */
    private final String snpName;

    /** Current snapshot state. */
    private final SnapshotRestoreState state;

    /** List of affected cache groups. */
    private final Map<String, Boolean> grps;

    /**
     * @param snpName Snapshot name.
     */
    public SnapshotRestoreRecord(String snpName, SnapshotRestoreState state, Map<String, Boolean> grps) {
        this.snpName = snpName;
        this.state = state;
        this.grps = grps;
    }

    /**
     * @param in Data input to create record from.
     * @throws IOException If fails.
     */
    public static SnapshotRestoreRecord read(DataInput in) throws IOException {
        int snpNameLen = in.readInt();
        byte[] snpNameBytes = new byte[snpNameLen];
        in.readFully(snpNameBytes);

        byte state0 = in.readByte();

        int grpCnt = in.readInt();

        HashMap<String, Boolean> grpMap = new HashMap<>(grpCnt);

        for (int i = 0; i < grpCnt; i++) {
            int grpNameLen = in.readInt();
            byte[] grpNameBytes = new byte[grpNameLen];
            in.readFully(grpNameBytes);

            byte shared = in.readByte();

            grpMap.put(new String(grpNameBytes), shared == 0 ? Boolean.FALSE : Boolean.TRUE);
        }

        return new SnapshotRestoreRecord(new String(snpNameBytes), SnapshotRestoreState.state(state0), grpMap);
    }

    /**
     * @param buf Buffer to write data into.
     */
    public void write(ByteBuffer buf) {
        byte[] nameBytes = snpName.getBytes();

        buf.putInt(nameBytes.length);
        buf.put(nameBytes);

        buf.put(state.state());

        buf.putInt(grps.size());

        for (Map.Entry<String, Boolean> e : grps.entrySet()) {
            byte[] grpNameBytes = e.getKey().getBytes();

            buf.putInt(grpNameBytes.length);
            buf.put(grpNameBytes);

            buf.put(e.getValue() ? (byte)1 : (byte)0);
        }
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Recorded snapshot state.
     */
    public SnapshotRestoreState state() {
        return state;
    }

    /**
     * @return List of cache groups to restore.
     */
    public Map<String, Boolean> cacheGroups() {
        return grps;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return SNAPSHOT_RESTORE_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        // Snapshot name length + Snapshot name bytes + State + Map entries number length
        int size = 4 + snpName.length() + 1 + 4;

        // Cache name length + Cache name bytes + Shared cache group flag
        for (Map.Entry<String, Boolean> e : grps.entrySet())
            size += 4 + e.getKey().getBytes().length + 1;

        return size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreRecord.class, this);
    }

    /**
     *
     */
    public enum SnapshotRestoreState {
        /** Restore process started. Copying files from snapshot directory. */
        START,

        /** All required caches copied. */
        FINISH;

        /**
         * @return State representation in byte.
         */
        public byte state() {
            return (byte)ordinal();
        }

        /**
         * @param state Byte state.
         * @return Resolved snapshot state.
         */
        public static SnapshotRestoreState state(byte state) {
            return values()[state];
        }
    }
}

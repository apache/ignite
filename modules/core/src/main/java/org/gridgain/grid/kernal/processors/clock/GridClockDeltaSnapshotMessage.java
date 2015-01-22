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

package org.gridgain.grid.kernal.processors.clock;

import org.gridgain.grid.kernal.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Message containing time delta map for all nodes.
 */
public class GridClockDeltaSnapshotMessage extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Snapshot version. */
    private GridClockDeltaVersion snapVer;

    /** Grid time deltas. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = long.class)
    private Map<UUID, Long> deltas;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridClockDeltaSnapshotMessage() {
        // No-op.
    }

    /**
     * @param snapVer Snapshot version.
     * @param deltas Deltas map.
     */
    public GridClockDeltaSnapshotMessage(GridClockDeltaVersion snapVer, Map<UUID, Long> deltas) {
        this.snapVer = snapVer;
        this.deltas = deltas;
    }

    /**
     * @return Snapshot version.
     */
    public GridClockDeltaVersion snapshotVersion() {
        return snapVer;
    }

    /**
     * @return Time deltas map.
     */
    public Map<UUID, Long> deltas() {
        return deltas;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClockDeltaSnapshotMessage _clone = new GridClockDeltaSnapshotMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClockDeltaSnapshotMessage _clone = (GridClockDeltaSnapshotMessage)_msg;

        _clone.snapVer = snapVer;
        _clone.deltas = deltas;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (deltas != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(deltas.size()))
                            return false;

                        commState.it = deltas.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<UUID, Long> e = (Map.Entry<UUID, Long>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putUuid(e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putLong(e.getValue()))
                            return false;

                        commState.keyDone = false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 1:
                if (!commState.putClockDeltaVersion(snapVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (deltas == null)
                        deltas = U.newHashMap(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            UUID _val = commState.getUuid();

                            if (_val == UUID_NOT_READ)
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        if (buf.remaining() < 8)
                            return false;

                        long _val = commState.getLong();

                        deltas.put((UUID)commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 1:
                GridClockDeltaVersion snapVer0 = commState.getClockDeltaVersion();

                if (snapVer0 == CLOCK_DELTA_VER_NOT_READ)
                    return false;

                snapVer = snapVer0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 59;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClockDeltaSnapshotMessage.class, this);
    }
}

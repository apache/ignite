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

package org.apache.ignite.internal.processors.clock;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Message containing time delta map for all nodes.
 */
public class GridClockDeltaSnapshotMessage extends MessageAdapter {
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
    @Override public MessageAdapter clone() {
        GridClockDeltaSnapshotMessage _clone = new GridClockDeltaSnapshotMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridClockDeltaSnapshotMessage _clone = (GridClockDeltaSnapshotMessage)_msg;

        _clone.snapVer = snapVer;
        _clone.deltas = deltas;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 0:
                if (!writer.writeMap("deltas", deltas, UUID.class, long.class))
                    return false;

                state++;

            case 1:
                if (!writer.writeMessage("snapVer", snapVer != null ? snapVer.clone() : null))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (state) {
            case 0:
                deltas = reader.readMap("deltas", UUID.class, long.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 1:
                snapVer = reader.readMessage("snapVer");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 60;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClockDeltaSnapshotMessage.class, this);
    }
}

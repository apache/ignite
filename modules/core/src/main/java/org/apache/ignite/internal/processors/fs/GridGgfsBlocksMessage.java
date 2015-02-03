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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * GGFS write blocks message.
 */
public class GridGgfsBlocksMessage extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** Batch id */
    private long id;

    /** Blocks to store. */
    @GridDirectMap(keyType = GridGgfsBlockKey.class, valueType = byte[].class)
    private Map<GridGgfsBlockKey, byte[]> blocks;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridGgfsBlocksMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileId File ID.
     * @param id Message id.
     * @param blocks Blocks to put in cache.
     */
    public GridGgfsBlocksMessage(IgniteUuid fileId, long id, Map<GridGgfsBlockKey, byte[]> blocks) {
        this.fileId = fileId;
        this.id = id;
        this.blocks = blocks;
    }

    /**
     * @return File id.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Batch id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Map of blocks to put in cache.
     */
    public Map<GridGgfsBlockKey, byte[]> blocks() {
        return blocks;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsBlocksMessage _clone = new GridGgfsBlocksMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsBlocksMessage _clone = (GridGgfsBlocksMessage)_msg;

        _clone.fileId = fileId;
        _clone.id = id;
        _clone.blocks = blocks;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (blocks != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(blocks.size()))
                            return false;

                        commState.it = blocks.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<GridGgfsBlockKey, byte[]> e = (Map.Entry<GridGgfsBlockKey, byte[]>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putMessage(e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putByteArray(e.getValue()))
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
                if (!commState.putGridUuid(fileId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(id))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 0:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (blocks == null)
                        blocks = U.newHashMap(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            Object _val = commState.getMessage();

                            if (_val == MSG_NOT_READ)
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        byte[] _val = commState.getByteArray();

                        if (_val == BYTE_ARR_NOT_READ)
                            return false;

                        blocks.put((GridGgfsBlockKey)commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 1:
                IgniteUuid fileId0 = commState.getGridUuid();

                if (fileId0 == GRID_UUID_NOT_READ)
                    return false;

                fileId = fileId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 67;
    }
}

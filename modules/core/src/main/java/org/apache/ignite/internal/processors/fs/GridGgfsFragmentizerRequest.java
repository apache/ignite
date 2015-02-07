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
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Fragmentizer request. Sent from coordinator to other GGFS nodes when colocated part of file
 * should be fragmented.
 */
public class GridGgfsFragmentizerRequest extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** Ranges to fragment. */
    @GridToStringInclude
    @GridDirectCollection(GridGgfsFileAffinityRange.class)
    private Collection<GridGgfsFileAffinityRange> fragmentRanges;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsFragmentizerRequest() {
        // No-op.
    }

    /**
     * @param fileId File id to fragment.
     * @param fragmentRanges Ranges to fragment.
     */
    public GridGgfsFragmentizerRequest(IgniteUuid fileId, Collection<GridGgfsFileAffinityRange> fragmentRanges) {
        this.fileId = fileId;
        this.fragmentRanges = fragmentRanges;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Fragment ranges.
     */
    public Collection<GridGgfsFileAffinityRange> fragmentRanges() {
        return fragmentRanges;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsFragmentizerRequest.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridGgfsFragmentizerRequest _clone = new GridGgfsFragmentizerRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsFragmentizerRequest _clone = (GridGgfsFragmentizerRequest)_msg;

        _clone.fileId = fileId;
        _clone.fragmentRanges = fragmentRanges;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putGridUuid("fileId", fileId))
                    return false;

                commState.idx++;

            case 1:
                if (fragmentRanges != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, fragmentRanges.size()))
                            return false;

                        commState.it = fragmentRanges.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putMessage(null, (GridGgfsFileAffinityRange)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

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
                fileId = commState.getGridUuid("fileId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                if (commState.readSize == -1) {
                    int _val = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                    commState.readSize = _val;
                }

                if (commState.readSize >= 0) {
                    if (fragmentRanges == null)
                        fragmentRanges = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridGgfsFileAffinityRange _val = (GridGgfsFileAffinityRange)commState.getMessage(null);

                        if (!commState.lastRead())
                            return false;

                        fragmentRanges.add((GridGgfsFileAffinityRange)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 69;
    }
}

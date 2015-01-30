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

import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;

import java.io.*;
import java.nio.*;

/**
 * Fragmentizer response.
 */
public class GridGgfsFragmentizerResponse extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File ID. */
    private IgniteUuid fileId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridGgfsFragmentizerResponse() {
        // No-op.
    }

    /**
     * @param fileId File ID.
     */
    public GridGgfsFragmentizerResponse(IgniteUuid fileId) {
        this.fileId = fileId;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsFragmentizerResponse _clone = new GridGgfsFragmentizerResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsFragmentizerResponse _clone = (GridGgfsFragmentizerResponse)_msg;

        _clone.fileId = fileId;
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

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 71;
    }
}

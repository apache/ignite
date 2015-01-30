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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job siblings response.
 */
public class GridJobSiblingsResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectTransient
    private Collection<ComputeJobSibling> siblings;

    /** */
    private byte[] siblingsBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobSiblingsResponse() {
        // No-op.
    }

    /**
     * @param siblings Siblings.
     * @param siblingsBytes Serialized siblings.
     */
    public GridJobSiblingsResponse(@Nullable Collection<ComputeJobSibling> siblings, @Nullable byte[] siblingsBytes) {
        this.siblings = siblings;
        this.siblingsBytes = siblingsBytes;
    }

    /**
     * @return Job siblings.
     */
    public Collection<ComputeJobSibling> jobSiblings() {
        return siblings;
    }

    /**
     * @param marsh Marshaller.
     * @throws IgniteCheckedException In case of error.
     */
    public void unmarshalSiblings(IgniteMarshaller marsh) throws IgniteCheckedException {
        assert marsh != null;

        if (siblingsBytes != null)
            siblings = marsh.unmarshal(siblingsBytes, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobSiblingsResponse _clone = new GridJobSiblingsResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridJobSiblingsResponse _clone = (GridJobSiblingsResponse)_msg;

        _clone.siblings = siblings;
        _clone.siblingsBytes = siblingsBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray("siblingsBytes", siblingsBytes))
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
                siblingsBytes = commState.getByteArray("siblingsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsResponse.class, this);
    }
}

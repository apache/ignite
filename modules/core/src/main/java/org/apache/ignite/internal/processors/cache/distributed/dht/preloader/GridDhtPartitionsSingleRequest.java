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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Request for single partition info.
 */
public class GridDhtPartitionsSingleRequest<K, V> extends GridDhtPartitionsAbstractMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsSingleRequest() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     */
    GridDhtPartitionsSingleRequest(GridDhtPartitionExchangeId id) {
        super(id, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtPartitionsSingleRequest _clone = new GridDhtPartitionsSingleRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

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

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 47;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsSingleRequest.class, this, super.toString());
    }
}

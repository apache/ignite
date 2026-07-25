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

package org.apache.ignite.internal.processors.igfs;


import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.nio.ByteBuffer;

/**
 * Block write request acknowledgement message.
 */
public class IgfsAckMessage extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    @Order(0)
    IgniteUuid fileId;

    /** Request ID to ack. */
    @Order(1)
    long id;

    /** Write exception. */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** */
    @Order(2)
    byte[] errBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsAckMessage() {
        // No-op.
    }

    /**
     * @param fileId File ID.
     * @param id Request ID.
     * @param err Error.
     */
    public IgfsAckMessage(IgniteUuid fileId, long id, @Nullable IgniteCheckedException err) {
        this.fileId = fileId;
        this.id = id;
        this.err = err;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Batch ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Error occurred when writing this batch, if any.
     */
    public IgniteCheckedException error() {
        return err;
    }



    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        super.prepareMarshal(marsh);

        if (err != null && errBytes == null)
            errBytes = U.marshal(marsh, err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(marsh, ldr);

        if (errBytes != null && err == null)
            err = U.unmarshal(marsh, errBytes, ldr);
    }
    /** {@inheritDoc} */
    @Override public short directType() {
        return 64;
    }

}

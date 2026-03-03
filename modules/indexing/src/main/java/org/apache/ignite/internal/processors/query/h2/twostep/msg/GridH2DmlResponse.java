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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Response to remote DML request.
 */
public class GridH2DmlResponse implements Message, GridCacheQueryMarshallable {
    /** Request id. */
    @GridToStringInclude
    @Order(0)
    long reqId;

    /** Number of updated rows. */
    @GridToStringInclude
    @Order(1)
    long updCnt;

    /** Error message. */
    @GridToStringInclude
    @Order(2)
    String err;

    /** Keys that failed. */
    @GridToStringInclude
    Object[] errKeys;

    /** Keys that failed (after marshalling). */
    @Order(3)
    byte[] errKeysBytes;

    /**
     * Default constructor.
     */
    public GridH2DmlResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param updCnt Updated row number.
     * @param errKeys Erroneous keys.
     * @param error Error message.
     */
    public GridH2DmlResponse(long reqId, long updCnt, Object[] errKeys, String error) {
        this.reqId = reqId;
        this.updCnt = updCnt;
        this.errKeys = errKeys;
        err = error;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updCnt;
    }

    /**
     * @return Error keys.
     */
    public Object[] errorKeys() {
        return errKeys;
    }

    /**
     * @return Error message.
     */
    public String error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void marshall(BinaryMarshaller m) {
        if (errKeysBytes != null || errKeys == null)
            return;

        try {
            errKeysBytes = U.marshal(m, errKeys);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public void unmarshall(GridKernalContext ctx) {
        if (errKeys != null || errKeysBytes == null)
            return;

        final ClassLoader ldr = U.resolveClassLoader(ctx.config());

        // To avoid deserializing of enum types.
        errKeys = BinaryUtils.rawArrayFromBinary(ctx.marshaller().binaryMarshaller().unmarshal(errKeysBytes, ldr));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2DmlResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -56;
    }

}


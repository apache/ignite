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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DataStreamerResponse implements Message {
    /** */
    @Order(value = 0, method = "requestId")
    private long reqId;

    /** */
    private @Nullable Throwable err;

    /** */
    @Order(value = 1, method = "errorBytes")
    private @Nullable byte[] errBytes;

    /**
     * @param reqId Request ID.
     * @param err Error.
     */
    public DataStreamerResponse(long reqId, @Nullable Throwable err) {
        this.reqId = reqId;
        this.err = err;
    }

    /**
     * Empty constructor.
     */
    public DataStreamerResponse() {
        // No-op.
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId Request ID.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Error bytes.
     */
    public @Nullable byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @param errBytes Error bytes.
     */
    public void errorBytes(@Nullable byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param marsh Marshaller.
     * @param log Logger.
     * @param marshErrBytes Marshalled error bytes.
     */
    public void prepareMarshal(Marshaller marsh, IgniteLogger log, byte[] marshErrBytes) {
        if (err != null && errBytes == null) {
            try {
                errBytes = U.marshal(marsh, err);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to marshal error [err=" + err + ", marshErr=" + e + ']', e);

                errBytes = marshErrBytes;
            }
        }
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (errBytes != null && err == null) {
            err = U.unmarshal(marsh, errBytes, ldr);

            errBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerResponse.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 63;
    }
}

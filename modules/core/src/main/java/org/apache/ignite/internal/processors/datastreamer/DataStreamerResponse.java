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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public class DataStreamerResponse implements Message {
    /** */
    @Order(value = 0, method = "requestId")
    private long reqId;

    /** */
    @Order(value = 1, method = "errorBytes")
    private byte[] errBytes;

    /**
     * @param reqId Request ID.
     * @param errBytes Error bytes.
     */
    public DataStreamerResponse(long reqId, byte[] errBytes) {
        this.reqId = reqId;
        this.errBytes = errBytes;
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
    public byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @param errBytes Error bytes.
     */
    public void errorBytes(byte[] errBytes) {
        this.errBytes = errBytes;
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

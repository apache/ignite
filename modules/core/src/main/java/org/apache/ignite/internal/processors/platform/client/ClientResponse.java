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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;

/**
 * Thin client response.
 */
public class ClientResponse extends ClientListenerResponse {
    /** Request id. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     */
    public ClientResponse(long reqId) {
        super(ClientStatus.SUCCESS, null);

        this.reqId = reqId;
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param err Error message.
     */
    public ClientResponse(long reqId, String err) {
        super(ClientStatus.FAILED, err);

        this.reqId = reqId;
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param status Status code.
     * @param err Error message.
     */
    public ClientResponse(long reqId, int status, String err) {
        super(status, err);

        this.reqId = reqId;
    }

    /**
     * Encodes the response data.
     */
    public void encode(BinaryRawWriterEx writer) {
        writer.writeLong(reqId);
        writer.writeInt(status());

        if (status() != ClientStatus.SUCCESS) {
            writer.writeString(error());
        }
    }

    /**
     * Gets the request id.
     *
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }
}

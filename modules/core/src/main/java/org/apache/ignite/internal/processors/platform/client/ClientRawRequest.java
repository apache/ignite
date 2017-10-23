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

/**
 * Raw request, produces a ClientResponse directly.
 */
public class ClientRawRequest extends ClientRequest {
    /** Status code. */
    private final int status;

    /** Message. */
    private final String msg;

    /**
     * Initializes a new instance of ClientRawRequest class.
     * @param reqId Request id.
     * @param msg Message.
     */
    public ClientRawRequest(long reqId, int status, String msg) {
        super(reqId);
        this.status = status;
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientResponse(requestId(), status, msg);
    }
}

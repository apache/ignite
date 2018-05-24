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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Thin client request.
 */
public class ClientRequest implements ClientListenerRequest {
    /** Request id. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientRequest(BinaryRawReader reader) {
        reqId = reader.readLong();
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     */
    public ClientRequest(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public long requestId() {
        return reqId;
    }

    /**
     * Processes the request.
     *
     * @return Response.
     */
    public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientResponse(reqId);
    }

    /**
     * Run the code with converting {@link SecurityException} to {@link IgniteClientException}.
     */
    protected static void runWithSecurityExceptionHandler(Runnable runnable) {
        try {
            runnable.run();
        }
        catch (SecurityException ex) {
            throw new IgniteClientException(
                ClientStatus.SECURITY_VIOLATION,
                "Client is not authorized to perform this operation",
                ex
            );
        }
    }

    /**
     * Authorize for specified permission.
     */
    protected void authorize(ClientConnectionContext ctx, SecurityPermission perm) {
        SecurityContext secCtx = ctx.securityContext();

        if (secCtx != null)
            runWithSecurityExceptionHandler(() -> ctx.kernalContext().security().authorize(null, perm, secCtx));
    }
}

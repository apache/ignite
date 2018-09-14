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

import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.security.SecurityContextHolder;

/**
 * Thin client request handler.
 */
public class ClientRequestHandler implements ClientListenerRequestHandler {
    /** Client context. */
    private final ClientConnectionContext ctx;

    /** Auth context. */
    private final AuthorizationContext authCtx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    ClientRequestHandler(ClientConnectionContext ctx, AuthorizationContext authCtx) {
        assert ctx != null;

        this.ctx = ctx;
        this.authCtx = authCtx;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handle(ClientListenerRequest req) {
        if (authCtx != null) {
            AuthorizationContext.context(authCtx);
            SecurityContextHolder.set(ctx.securityContext());
        }

        try {
            return ((ClientRequest)req).process(ctx);
        }
        finally {
            if (authCtx != null)
                AuthorizationContext.clear();

            SecurityContextHolder.clear();
        }
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handleException(Exception e, ClientListenerRequest req) {
        assert req != null;
        assert e != null;

        int status = e instanceof IgniteClientException ?
            ((IgniteClientException)e).statusCode() : ClientStatus.FAILED;

        return new ClientResponse(req.requestId(), status, e.getMessage());
    }

    /** {@inheritDoc} */
    @Override public void writeHandshake(BinaryWriterExImpl writer) {
        writer.writeBoolean(true);
    }
}
/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.VER_1_4_0;

/**
 * Thin client request handler.
 */
public class ClientRequestHandler implements ClientListenerRequestHandler {
    /** Client context. */
    private final ClientConnectionContext ctx;

    /** Auth context. */
    private final AuthorizationContext authCtx;

    /** Protocol version. */
    ClientListenerProtocolVersion ver;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param authCtx Authentication context.
     * @param ver Protocol version.
     */
    ClientRequestHandler(ClientConnectionContext ctx, AuthorizationContext authCtx, ClientListenerProtocolVersion ver) {
        assert ctx != null;

        this.ctx = ctx;
        this.authCtx = authCtx;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handle(ClientListenerRequest req) {
        try {
            return ((ClientRequest)req).process(ctx);
        }
        catch (SecurityException ex) {
            throw new IgniteClientException(
                ClientStatus.SECURITY_VIOLATION,
                "Client is not authorized to perform this operation",
                ex
            );
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

        if (ver.compareTo(VER_1_4_0) >= 0) {
            writer.writeUuid(ctx.kernalContext().localNodeId());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancellationCommand(int cmdId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancellationSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void registerRequest(long reqId, int cmdType) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unregisterRequest(long reqId) {
        // No-op.
    }
}

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.plugin.security.SecurityException;

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
    }

    /** {@inheritDoc} */
    @Override public boolean isCancellationCommand(int cmdId) {
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
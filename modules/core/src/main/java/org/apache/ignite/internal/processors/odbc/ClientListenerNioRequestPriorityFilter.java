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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContextHolder;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Filter that checks whether message should be handled synchronously with high priority
 * and calls corresponding handle method with specified {@link GridNioParser}.
 */
public class ClientListenerNioRequestPriorityFilter extends GridNioFilterAdapter {
    /** Grid logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /**
     * Constructor
     */
    public ClientListenerNioRequestPriorityFilter(IgniteLogger log) {
        super("GridNioPriorityQueryFilter");

        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override
    public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        return proceedSessionWrite(ses, msg, fut, ackC);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        ClientListenerConnectionContext connCtx = ses.meta(ClientListenerNioListener.CONN_CTX_META_KEY);

        if (connCtx != null && connCtx.parser() != null) {
            byte[] inMsg;

            int cmdId;

            try {
                inMsg = (byte[])msg;

                cmdId = connCtx.parser().decodeCommandType(inMsg);
            }
            catch (Exception e) {
                U.error(log, "Failed to parse client request.", e);

                ses.close();

                return;
            }

            if (connCtx.handler().isSynchronousHandlingExpected(cmdId)) {

                ClientListenerRequest req;
                try {
                    req = connCtx.parser().decode(inMsg);
                }
                catch (Exception e) {
                    U.error(log, "Failed to parse client request.", e);

                    ses.close();

                    return;
                }
                try {
                    long startTime = 0;

                    if (log.isDebugEnabled()) {
                        startTime = System.nanoTime();

                        log.debug("Client request received [reqId=" + req.requestId() + ", addr=" +
                            ses.remoteAddress() + ", req=" + req + ']');
                    }

                    ClientListenerResponse resp;

                    AuthorizationContext authCtx = connCtx.authorizationContext();
                    SecurityContext oldSecCtx = SecurityContextHolder.push(connCtx.securityContext());

                    if (authCtx != null)
                        AuthorizationContext.context(authCtx);

                    try {
                        resp = connCtx.handler().handleSynchronously(req);
                    }
                    finally {
                        SecurityContextHolder.pop(oldSecCtx);

                        if (authCtx != null)
                            AuthorizationContext.clear();
                    }

                    if (resp != null) {
                        if (log.isDebugEnabled()) {
                            long dur = (System.nanoTime() - startTime) / 1000;

                            log.debug("Client request processed [reqId=" + req.requestId() + ", dur(mcs)=" + dur +
                                ", resp=" + resp.status() + ']');
                        }

                        ses.send(connCtx.parser().encode(resp));
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to process client request [req=" + req + ']', e);

                    ses.send(connCtx.parser().encode(connCtx.handler().handleException(e, req)));
                }
            }
            else
                proceedMessageReceived(ses, msg);
        }
        else
            proceedMessageReceived(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }
}
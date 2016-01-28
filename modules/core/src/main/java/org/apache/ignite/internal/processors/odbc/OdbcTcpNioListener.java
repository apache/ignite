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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for ODBC driver connection.
 */
public class OdbcTcpNioListener extends GridNioServerListenerAdapter<OdbcRequest> {
    /** Logger. */
    private final IgniteLogger log;

    /** Command handler. */
    private final OdbcCommandHandler handler;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /**
     * @param log Logger.
     * @param hnd Protocol handler.
     */
    OdbcTcpNioListener(final IgniteLogger log, final OdbcCommandHandler hnd, final GridSpinBusyLock busyLock) {
        this.log = log;
        this.handler = hnd;
        this.busyLock = busyLock;
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("Driver connected");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled())
            log.debug("Driver disconnected");

        if (e != null) {
            if (e instanceof RuntimeException)
                U.error(log, "Failed to process request from remote client: " + ses, e);
            else
                U.warn(log, "Closed client session due to exception [ses=" + ses + ", msg=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, OdbcRequest msg) {
        assert msg != null;

        if (log.isDebugEnabled())
            log.debug("Received request from client: [msg=" + msg + ']');

        OdbcResponse res = handle(msg);

        if (log.isDebugEnabled())
            log.debug("Handling result: [res=" + res.status() + ']');

        ses.send(res);
    }

    /**
     * Handle incoming ODBC request.
     *
     * @param req ODBC request.
     * @return ODBC response.
     */
    OdbcResponse handle(OdbcRequest req) {
        assert handler != null;

        if (!busyLock.enterBusy()) {
            String errMsg = "Failed to handle request [req=" + req +
                    ", err=Received request while stopping grid]";

            U.error(log, errMsg);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, errMsg);
        }

        try {
            return handler.handle(req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }
}

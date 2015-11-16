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

package org.apache.ignite.internal.processors.odbc.protocol;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.GridOdbcProtocolHandler;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for ODBC driver connection.
 */
public class GridTcpOdbcNioListener extends GridNioServerListenerAdapter<GridOdbcRequest> {
    /** Server. */
    private GridTcpOdbcServer srv;

    /** Logger. */
    protected final IgniteLogger log;

    /** Context. */
    protected final GridKernalContext ctx;

    /** Protocol handler. */
    private GridOdbcProtocolHandler hnd;

    GridTcpOdbcNioListener(IgniteLogger log, GridTcpOdbcServer srv, GridKernalContext ctx, GridOdbcProtocolHandler hnd) {
        this.log = log;
        this.srv = srv;
        this.ctx = ctx;
        this.hnd = hnd;
    }

    @Override
    public void onConnected(GridNioSession ses) {
        System.out.println("Driver connected");
    }

    @Override
    public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        System.out.println("Driver disconnected");

        if (e != null) {
            if (e instanceof RuntimeException)
                U.error(log, "Failed to process request from remote client: " + ses, e);
            else
                U.warn(log, "Closed client session due to exception [ses=" + ses + ", msg=" + e.getMessage() + ']');
        }
    }

    @Override
    public void onMessage(GridNioSession ses, GridOdbcRequest msg) {
        assert msg != null;

        System.out.println("Query: " + msg.command());

        hnd.handleAsync(msg).listen(new CI1<IgniteInternalFuture<GridOdbcResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridOdbcResponse> fut) {
                GridOdbcResponse res;

                try {
                    res = fut.get();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to process client request: " + msg, e);

                    res = new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                        "Failed to process client request: " + e.getMessage());
                }

                System.out.println("Resulting success status: " + res.getSuccessStatus());

                GridNioFuture<?> sf = ses.send(res);

                // Check if send failed.
                if (sf.isDone())
                    try {
                        sf.get();
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to process client request [ses=" + ses + ", msg=" + msg + ']', e);
                    }
            }
        });
    }
}

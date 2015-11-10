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

package org.apache.ignite.internal.processors.query.odbc;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for ODBC driver connection.
 */
public class GridTcpOdbcNioListener extends GridNioServerListenerAdapter<byte[]> {

    /** Server. */
    private GridTcpOdbcServer srv;

    /** Logger. */
    protected final IgniteLogger log;

    /** Context. */
    protected final GridKernalContext ctx;

    GridTcpOdbcNioListener(IgniteLogger logger, GridTcpOdbcServer server, GridKernalContext context) {
        log = logger;
        srv = server;
        ctx = context;
    }

    @Override
    public void onConnected(GridNioSession ses) {
        System.out.println("onConnected");
    }

    @Override
    public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        System.out.println("onDisconnected");
    }

    @Override
    public void onMessage(GridNioSession ses, byte[] msg) {
        System.out.println("onMessage");
        System.out.println("length: " + msg.length);
        for (int i = 0; i < msg.length && i < 32; ++i) {
            System.out.println("msg[" + i + "] = " + msg[i]);
        }
    }
}

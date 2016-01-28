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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.marshaller.Marshaller;

/**
 * ODBC processor.
 */
public class OdbcProcessor extends GridProcessorAdapter {
    /** OBCD TCP Server. */
    private OdbcTcpServer srv;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * @param ctx Kernal context.
     */
    public OdbcProcessor(GridKernalContext ctx) {
        super(ctx);

        srv = new OdbcTcpServer(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (isOdbcEnabled()) {
            Marshaller marsh = ctx.config().getMarshaller();

            if (marsh != null && !(marsh instanceof BinaryMarshaller))
                throw new IgniteCheckedException("ODBC may only be used with BinaryMarshaller.");

            srv.start(new OdbcCommandHandler(ctx), busyLock);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (isOdbcEnabled()) {
            srv.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (isOdbcEnabled()) {
            if (log.isDebugEnabled())
                log.debug("ODBC processor started.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (isOdbcEnabled()) {
            busyLock.block();

            if (log.isDebugEnabled())
                log.debug("ODBC processor stopped.");
        }
    }

    /**
     * Check if the ODBC is enabled.
     *
     * @return Whether or not ODBC is enabled.
     */
    public boolean isOdbcEnabled() {
        return ctx.config().getOdbcConfiguration() != null;
    }
}

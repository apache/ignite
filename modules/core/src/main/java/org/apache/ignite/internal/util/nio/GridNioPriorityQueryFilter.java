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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.odbc.ClientListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Filter that checks whether message should be handled synchronously with high priority
 * and calls corresponding handle method with specified {@link GridNioParser}.
 */
public class GridNioPriorityQueryFilter extends GridNioFilterAdapter {

    /**
     * Constructor
     */
    public GridNioPriorityQueryFilter() {
        super("GridNioPriorityQueryFilter");
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

        if (connCtx != null && connCtx.parser() != null){
            byte[] inMsg = (byte[]) msg;

            int cmdId = connCtx.parser().decodeCommandType(inMsg);
            if (connCtx.handler().isSynchronousHandlingExpected(cmdId)){
                ClientListenerResponse resp = connCtx.handler().handleSynchronously(connCtx.parser().decode(inMsg));

                if (resp != null) {
                    byte[] outMsg = connCtx.parser().encode(resp);

                    ses.send(outMsg);
                }
            } else
                proceedMessageReceived(ses, msg);
        } else
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
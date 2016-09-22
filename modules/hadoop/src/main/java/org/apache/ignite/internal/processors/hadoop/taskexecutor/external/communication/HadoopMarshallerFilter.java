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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Serialization filter.
 */
public class HadoopMarshallerFilter extends GridNioFilterAdapter {
    /** Marshaller. */
    private Marshaller marshaller;

    /**
     * @param marshaller Marshaller to use.
     */
    public HadoopMarshallerFilter(Marshaller marshaller) {
        super("HadoopMarshallerFilter");

        this.marshaller = marshaller;
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
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws IgniteCheckedException {
        assert msg instanceof HadoopMessage : "Invalid message type: " + msg;

        return proceedSessionWrite(ses, marshaller.marshal(msg));
    }

    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        assert msg instanceof byte[];

        // Always unmarshal with system classloader.
        proceedMessageReceived(ses, marshaller.unmarshal((byte[])msg, null));
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
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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
final class WriteRequestImpl implements SessionWriteRequest, SessionChangeRequest {
    /** */
    private GridNioSession ses;

    /** */
    private final Object msg;

    /** */
    private boolean msgThread;

    /** */
    private final boolean skipRecovery;

    /** */
    private final IgniteInClosure<IgniteException> ackC;

    /**
     * @param ses Session.
     * @param msg Message.
     * @param skipRecovery Skip recovery flag.
     * @param ackC Closure invoked when message ACK is received.
     */
    WriteRequestImpl(GridNioSession ses,
        Object msg,
        boolean skipRecovery,
        IgniteInClosure<IgniteException> ackC) {
        this.ses = ses;
        this.msg = msg;
        this.skipRecovery = skipRecovery;
        this.ackC = ackC;
    }

    /** {@inheritDoc} */
    @Override public void messageThread(boolean msgThread) {
        this.msgThread = msgThread;
    }

    /** {@inheritDoc} */
    @Override public boolean messageThread() {
        return msgThread;
    }

    /** {@inheritDoc} */
    @Override public boolean skipRecovery() {
        return skipRecovery;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        assert msg instanceof Message;

        ((Message)msg).onAckReceived();
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<IgniteException> ackClosure() {
        return ackC;
    }

    /** {@inheritDoc} */
    @Override public void onError(Exception e) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Object message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public void onMessageWritten() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void resetSession(GridNioSession ses) {
        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public NioOperation operation() {
        return NioOperation.REQUIRE_WRITE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WriteRequestImpl.class, this);
    }
}

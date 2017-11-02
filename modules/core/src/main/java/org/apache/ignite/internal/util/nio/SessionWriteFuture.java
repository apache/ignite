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

final class SessionWriteFuture extends NioOperationFuture<Void> implements SessionWriteRequest {
    /** Message. */
    private Object msg;

    /** */
    private boolean skipRecovery;

    /** System message flag. */
    private boolean system;

    SessionWriteFuture(GridSelectorNioSessionImpl ses, Object msg, boolean skipRecovery, boolean system){
        super(ses, NioOperation.REQUIRE_WRITE, null);

        this.msg = msg;
        this.skipRecovery = skipRecovery;
        this.system = system;

        messageThread(system);
    }

    SessionWriteFuture(GridSelectorNioSessionImpl ses, Object msg, IgniteInClosure<IgniteException> ackC){
        super(ses, NioOperation.REQUIRE_WRITE, ackC);

        this.msg = msg;
    }

    SessionWriteFuture(GridSelectorNioSessionImpl ses, Object msg, boolean skipRecovery, IgniteInClosure<IgniteException> ackC){
        super(ses, NioOperation.REQUIRE_WRITE, ackC);

        this.msg = msg;
        this.skipRecovery = skipRecovery;
    }

    /** {@inheritDoc} */
    @Override public <T> void invoke(GridNioServer<T> nio, GridNioWorker worker) {
        if (ses.worker() == worker)
            worker.registerWrite(ses);
    }

    /** {@inheritDoc} */
    @Override public Object message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public void resetSession(GridNioSession ses) {
        assert msg instanceof Message : msg;

        this.ses = (GridSelectorNioSessionImpl)ses;
    }

    /** {@inheritDoc} */
    @Override public void onError(Exception e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        assert !system && msg instanceof Message : msg;

        ((Message)msg).onAckReceived();
    }

    /** {@inheritDoc} */
    @Override public void onMessageWritten() {
        onDone();
    }

    /** {@inheritDoc} */
    @Override public boolean skipRecovery() {
        return skipRecovery;
    }

    /** {@inheritDoc} */
    @Override public boolean system() {
        return system;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionWriteFuture.class, this, super.toString());
    }
}

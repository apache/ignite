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

import java.nio.channels.SocketChannel;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Class for requesting write and session close operations.
 */
class NioOperationFuture<R> extends GridNioFutureImpl<R> implements SessionWriteRequest,
    SessionChangeRequest, GridNioKeyAttachment {
    /** Socket channel in register request. */
    @GridToStringExclude
    private SocketChannel sockCh;

    /** Session to perform operation on. */
    @GridToStringExclude
    private GridSelectorNioSessionImpl ses;

    /** Is it a close request or a write request. */
    private NioOperation op;

    /** Message. */
    private Object msg;

    /** */
    @GridToStringExclude
    private boolean accepted;

    /** */
    @GridToStringExclude
    private Map<Integer, ?> meta;

    /** */
    @GridToStringExclude
    private boolean skipRecovery;

    /**
     * @param sockCh Socket channel.
     * @param accepted {@code True} if socket has been accepted.
     * @param meta Optional meta.
     */
    NioOperationFuture(
        SocketChannel sockCh,
        boolean accepted,
        @Nullable Map<Integer, ?> meta
    ) {
        super(null);

        op = NioOperation.REGISTER;

        this.sockCh = sockCh;
        this.accepted = accepted;
        this.meta = meta;
    }

    /**
     * Creates change request.
     *
     * @param ses Session to change.
     * @param op Requested operation.
     */
    NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op) {
        super(null);

        assert ses != null || op == NioOperation.DUMP_STATS : "Invalid params [ses=" + ses + ", op=" + op + ']';
        assert op != null;
        assert op != NioOperation.REGISTER;

        this.ses = ses;
        this.op = op;
    }

    /**
     * Creates change request.
     *
     * @param ses Session to change.
     * @param op Requested operation.
     * @param msg Message.
     * @param ackC Closure invoked when message ACK is received.
     */
    NioOperationFuture(GridSelectorNioSessionImpl ses,
        NioOperation op,
        Object msg,
        IgniteInClosure<IgniteException> ackC) {
        super(ackC);

        assert ses != null;
        assert op != null;
        assert op != NioOperation.REGISTER;
        assert msg != null;

        this.ses = ses;
        this.op = op;
        this.msg = msg;
    }

    /**
     * Creates change request.
     *
     * @param ses Session to change.
     * @param op Requested operation.
     * @param commMsg Direct message.
     * @param skipRecovery Skip recovery flag.
     * @param ackC Closure invoked when message ACK is received.
     */
    NioOperationFuture(GridSelectorNioSessionImpl ses,
        NioOperation op,
        Message commMsg,
        boolean skipRecovery,
        IgniteInClosure<IgniteException> ackC) {
        super(ackC);

        assert ses != null;
        assert op != null;
        assert op != NioOperation.REGISTER;
        assert commMsg != null;

        this.ses = ses;
        this.op = op;
        this.msg = commMsg;
        this.skipRecovery = skipRecovery;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSession() {
        return ses != null;
    }

    /** {@inheritDoc} */
    public NioOperation operation() {
        return op;
    }

    public void operation(NioOperation op) {
        this.op = op;
    }

    /** {@inheritDoc} */
    public Object message() {
        return msg;
    }

    public void message(Object msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    public void resetSession(GridNioSession ses) {
        assert msg instanceof Message : msg;

        this.ses = (GridSelectorNioSessionImpl)ses;
    }

    /**
     * @return Socket channel for register request.
     */
    SocketChannel socketChannel() {
        return sockCh;
    }

    /** {@inheritDoc} */
    public GridSelectorNioSessionImpl session() {
        return ses;
    }

    /**
     * @return {@code True} if connection has been accepted.
     */
    boolean accepted() {
        return accepted;
    }

    /**
     * @return Meta.
     */
    public Map<Integer, ?> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void onError(Exception e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        assert msg instanceof Message : msg;

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
    @Override public String toString() {
        return S.toString(NioOperationFuture.class, this);
    }
}

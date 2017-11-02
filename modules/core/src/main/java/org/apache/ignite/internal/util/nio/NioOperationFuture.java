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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Class for requesting session write and change operations.
 */
@SuppressWarnings("unchecked")
abstract class NioOperationFuture<R> extends GridNioFutureImpl<R> implements SessionChangeRequest, GridNioKeyAttachment {
    /** Session to perform operation on. */
    @GridToStringExclude
    protected GridSelectorNioSessionImpl ses;

    /** Is it a close request or a write request. */
    protected NioOperation op;

    /**
     * Creates change request.
     *
     * @param ses Session to change.
     * @param op Requested operation.
     */
    NioOperationFuture(GridSelectorNioSessionImpl ses, NioOperation op) {
        super(null);

        this.ses = ses;
        this.op = op;
    }

    /**
     * Creates change request.
     *
     * @param ses Session to change.
     * @param op Requested operation.
     * @param ackC Closure invoked when message ACK is received.
     */
    NioOperationFuture(GridSelectorNioSessionImpl ses,
        NioOperation op,
        IgniteInClosure<IgniteException> ackC) {
        super(ackC);

        this.ses = ses;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSession() {
        return ses != null;
    }

    /** {@inheritDoc} */
    @Override public NioOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public GridSelectorNioSessionImpl session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NioOperationFuture.class, this);
    }
}

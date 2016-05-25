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
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Default future implementation.
 */
public class GridNioFutureImpl<R> extends GridFutureAdapter<R> implements GridNioFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected boolean msgThread;

    /** */
    protected IgniteInClosure<IgniteException> ackClosure;

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
        return false;
    }

    /** {@inheritDoc} */
    @Override public void ackClosure(IgniteInClosure<IgniteException> closure) {
        ackClosure = closure;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<IgniteException> ackClosure() {
        return ackClosure;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioFutureImpl.class, this);
    }
}
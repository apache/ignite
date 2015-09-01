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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Future that represents already completed result.
 */
public class GridNioFinishedFuture<R> extends GridFinishedFuture<R> implements GridNioFuture<R> {
    /** Message thread flag. */
    private boolean msgThread;

    /**
     * @param res Result.
     */
    public GridNioFinishedFuture(R res) {
        super(res);
    }

    /**
     * @param err Error.
     */
    public GridNioFinishedFuture(Throwable err) {
        super(err);
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
        return true;
    }

    /** {@inheritDoc} */
    @Override public void ackClosure(IgniteInClosure<IgniteException> closure) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInClosure<IgniteException> ackClosure() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioFinishedFuture.class, this, super.toString());
    }
}
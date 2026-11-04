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

package org.apache.ignite.spi.communication.tcp.internal;

import java.nio.channels.SelectableChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Callback that is intended to be executed after timeout on handshake.
 * <p>
 * The deadline outcome is a single atomic state, so a caller can unambiguously distinguish a deadline that
 * actually fired ({@link #timedOut()} is {@code true} and the timeout task closes the connection) from a
 * deadline that was merely cancelled before it fired: a lost {@link #cancel()} call only ever reports a
 * fired deadline, never an ambiguous "the task could no longer be cancelled" state.
 */
public class HandshakeTimeoutObject implements Runnable {
    /** State: the deadline is armed and not yet settled. */
    private static final int PENDING = 0;

    /** State: the deadline was cancelled before it fired. */
    private static final int CANCELLED = 1;

    /** State: the deadline fired and the timeout task closes the connection. */
    private static final int TIMED_OUT = 2;

    /** Socket channel that is closed when the deadline fires. */
    private final SelectableChannel channel;

    /** Deadline state, transitions {@link #PENDING} to {@link #CANCELLED} or {@link #TIMED_OUT}. */
    private final AtomicInteger state = new AtomicInteger(PENDING);

    /**
     * @param channel Socket channel to close when the deadline fires.
     */
    public HandshakeTimeoutObject(SelectableChannel channel) {
        assert channel != null;

        this.channel = channel;
    }

    /**
     * @return {@code True} if the deadline was cancelled before it fired.
     */
    public boolean cancel() {
        return state.compareAndSet(PENDING, CANCELLED);
    }

    /**
     * @return {@code True} if the deadline actually fired and the timeout task is closing the connection.
     *         The state is set before the close runs, so the close may still be in progress when this
     *         returns {@code true}.
     */
    public boolean timedOut() {
        return state.get() == TIMED_OUT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeTimeoutObject.class, this);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (state.compareAndSet(PENDING, TIMED_OUT)) {
            // Close socket - timeout occurred.
            U.closeQuiet(channel);
        }
    }
}

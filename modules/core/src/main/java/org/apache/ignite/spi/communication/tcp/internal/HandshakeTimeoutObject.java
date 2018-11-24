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
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public class HandshakeTimeoutObject<T> implements IgniteSpiTimeoutObject {
    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private final T obj;

    /** */
    private final long endTime;

    /** */
    private final AtomicBoolean done = new AtomicBoolean();

    /**
     * @param obj Client.
     * @param endTime End time.
     */
    public HandshakeTimeoutObject(T obj, long endTime) {
        assert obj != null;
        assert obj instanceof GridCommunicationClient || obj instanceof SelectableChannel;
        assert endTime > 0;

        this.obj = obj;
        this.endTime = endTime;
    }

    /**
     * @return {@code True} if object has not yet been timed out.
     */
    public boolean cancel() {
        return done.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (done.compareAndSet(false, true)) {
            // Close socket - timeout occurred.
            if (obj instanceof GridCommunicationClient)
                ((GridCommunicationClient)obj).forceClose();
            else
                U.closeQuiet((AutoCloseable)obj);
        }
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeTimeoutObject.class, this);
    }
}

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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Callback that is intended to be executed after timeout on handshake.
 */
public class HandshakeTimeoutObject implements Runnable {
    /** Object which used for connection. */
    private final Object connectionObj;

    /** Done. */
    private final AtomicBoolean done = new AtomicBoolean();

    /**
     * @param connectionObj Client.
     */
    public HandshakeTimeoutObject(Object connectionObj) {
        assert connectionObj != null;
        assert connectionObj instanceof GridCommunicationClient || connectionObj instanceof SelectableChannel;

        this.connectionObj = connectionObj;
    }

    /**
     * @return {@code True} if object has not yet been timed out.
     */
    public boolean cancel() {
        return done.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeTimeoutObject.class, this);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (done.compareAndSet(false, true)) {
            // Close socket - timeout occurred.
            if (connectionObj instanceof GridCommunicationClient)
                ((GridCommunicationClient)connectionObj).forceClose();
            else
                U.closeQuiet((AutoCloseable)connectionObj);
        }
    }
}

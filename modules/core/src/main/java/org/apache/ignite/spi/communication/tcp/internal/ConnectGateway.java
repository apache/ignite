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

import java.io.Serializable;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Lock and error control work flow.
 */
public class ConnectGateway implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 8798048262450037107L;

    /** Lock. */
    private GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

    /** Err. */
    private IgniteException err;

    /**
     * Enter to critical section.
     */
    public void enter() {
        lock.readLock();

        if (err != null) {
            lock.readUnlock();

            throw err;
        }
    }

    /**
     * @return {@code True} if entered gateway.
     */
    public boolean tryEnter() {
        lock.readLock();

        boolean res = err == null;

        if (!res)
            lock.readUnlock();

        return res;
    }

    /**
     * Leave critical section.
     */
    public void leave() {
        lock.readUnlock();
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void disconnected(IgniteFuture<?> reconnectFut) {
        lock.writeLock();

        err = new IgniteClientDisconnectedException(reconnectFut, "Failed to connect, client node disconnected.");

        lock.writeUnlock();
    }

    /**
     * Reset error.
     */
    public void reconnected() {
        lock.writeLock();

        try {
            if (err instanceof IgniteClientDisconnectedException)
                err = null;
        }
        finally {
            lock.writeUnlock();
        }
    }

    /**
     * Add error to this class.
     */
    public void stopped() {
        lock.readLock();

        err = new IgniteException("Failed to connect, node stopped.");

        lock.readUnlock();
    }
}

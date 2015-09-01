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

package org.apache.ignite;

import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown from Ignite API when client node disconnected from cluster.
 */
public class IgniteClientDisconnectedException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteFuture<?> reconnectFut;

    /**
     * @param reconnectFut Reconnect future.
     * @param msg Error message.
     */
    public IgniteClientDisconnectedException(IgniteFuture<?> reconnectFut, String msg) {
        this(reconnectFut, msg, null);
    }

    /**
     * @param reconnectFut Reconnect future.
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteClientDisconnectedException(
        IgniteFuture<?> reconnectFut,
        String msg,
        @Nullable Throwable cause) {
        super(msg, cause);

        this.reconnectFut = reconnectFut;
    }

    /**
     * @return Future that will be completed when client reconnected.
     */
    public IgniteFuture<?> reconnectFuture() {
        return reconnectFut;
    }
}
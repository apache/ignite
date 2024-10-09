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

package org.apache.ignite.internal;

import org.apache.ignite.ClientContext;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.jetbrains.annotations.Nullable;

/**
 * Holds {@link ClientContext} for threads.
 *
 * @see ClientContext
 * @see JdbcThinConnection#setClientInfo
 */
public class ClientContextInternal implements AutoCloseable {
    /** Holds client context for current thread. */
    private static final ThreadLocal<ClientContext> ctx = new ThreadLocal<>();

    /**
     * Set context for current thread.
     *
     * @param clnCtx Client context to set.
     * @return Client context if specified, otherwise {@code null}.
     */
    public static @Nullable ClientContextInternal withClientContext(@Nullable ClientContext clnCtx) {
        if (clnCtx == null)
            return null;

        assert ctx.get() == null : Thread.currentThread().getName();

        ctx.set(clnCtx);

        return new ClientContextInternal();
    }

    /** @return Client context for current thread. */
    public static @Nullable ClientContext clientContext() {
        return ctx.get();
    }

    /** Unset context for current thread. */
    @Override public void close() {
        ctx.set(null);
    }
}

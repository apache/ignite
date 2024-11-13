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

package org.apache.ignite.internal.cache.context;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.SessionContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.jetbrains.annotations.Nullable;

/** Processor for handling session context. */
public class SessionContextProcessor extends GridProcessorAdapter {
    /** Holds session context for current thread. */
    private final ThreadLocal<SessionContext> ctx = new ThreadLocal<>();

    /** */
    public SessionContextProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Set context for current thread.
     *
     * @param sesAttrs Session attributes to set.
     */
    public AutoCloseable withContext(@Nullable Map<String, String> sesAttrs) {
        if (sesAttrs == null)
            return null;

        SessionContextCloseable sesCtx = new SessionContextCloseable(sesAttrs);

        ctx.set(sesCtx);

        return sesCtx;
    }

    /** @return Session context for current thread. */
    public @Nullable SessionContext context() {
        return ctx.get();
    }

    /** */
    public void clear() {
        ctx.remove();
    }

    public @Nullable Map<String, String> attributes() {
        SessionContextCloseable ses = (SessionContextCloseable)ctx.get();

        return ses == null ? null : ses.attrs;
    }

    /** */
    private class SessionContextCloseable implements SessionContext, AutoCloseable {
        /** Session attributes. */
        private final Map<String, String> attrs;

        /** @param attrs Session attributes. */
        public SessionContextCloseable(Map<String, String> attrs) {
            this.attrs = new HashMap<>(attrs);
        }

        /** {@inheritDoc} */
        @Override public @Nullable String getAttribute(String name) {
            return attrs.get(name);
        }

        /** Clears thread local session context. */
        @Override public void close() {
            ctx.remove();
        }
    }
}

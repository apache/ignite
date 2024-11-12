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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.ApplicationContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.jetbrains.annotations.Nullable;

/** Processor for handling application context set by user. */
public class ApplicationContextProcessor extends GridProcessorAdapter {
    /** Holds application context for current thread. */
    private final ThreadLocal<ApplicationContext> ctx = new ThreadLocal<>();

    /** */
    public ApplicationContextProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Set context for current thread.
     *
     * @param appAttrs Application attributes to set.
     */
    public AutoCloseable withApplicationContext(@Nullable Map<String, String> appAttrs) {
        if (appAttrs == null)
            return null;

        ApplicationContextCloseable appCtx = new ApplicationContextCloseable(appAttrs);

        ctx.set(appCtx);

        return appCtx;
    }

    /** @return Application context for current thread. */
    public @Nullable ApplicationContext applicationContext() {
        return ctx.get();
    }

    /** */
    private class ApplicationContextCloseable implements ApplicationContext, AutoCloseable {
        /** Application attributes. */
        private final Map<String, String> attrs;

        /** @param attrs Application attributes. */
        public ApplicationContextCloseable(Map<String, String> attrs) {
            this.attrs = new HashMap<>(attrs);
        }

        /** */
        @Override public Map<String, String> getAttributes() {
            return Collections.unmodifiableMap(attrs);
        }

        /** */
        @Override public void close() {
            ctx.remove();
        }
    }
}

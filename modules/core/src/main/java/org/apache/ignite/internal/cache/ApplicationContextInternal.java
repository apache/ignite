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

package org.apache.ignite.internal.cache;

import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/** Hold application attribute set by user with {@link IgniteCache#withApplicationAttributes}. */
public class ApplicationContextInternal implements AutoCloseable {
    /** Holds application context for current thread. */
    private static final ThreadLocal<ApplicationContextInternal> ctx = new ThreadLocal<>();

    /** Application attributes. */
    private final Map<String, String> attrs;

    /** @param attrs Application attributes. */
    private ApplicationContextInternal(Map<String, String> attrs) {
        this.attrs = attrs;
    }

    /**
     * Set context for current thread.
     *
     * @param attrs Application attributes to set.
     */
    public static ApplicationContextInternal withApplicationAttributes(@Nullable Map<String, String> attrs) {
        if (attrs == null)
            return null;

        ApplicationContextInternal appCtx = new ApplicationContextInternal(attrs);

        ctx.set(appCtx);

        return appCtx;
    }

    /** @return Application context for current thread. */
    public static @Nullable Map<String, String> attributes() {
        ApplicationContextInternal appCtx = ctx.get();

        return appCtx == null ? null : appCtx.attrs;
    }

    /** Unset context for current thread. */
    @Override public void close() {
        ctx.remove();
    }
}

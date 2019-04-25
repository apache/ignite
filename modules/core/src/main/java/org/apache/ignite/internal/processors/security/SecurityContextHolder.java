/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security;

import org.jetbrains.annotations.Nullable;

/**
 * Thread-local security context.
 */
public class SecurityContextHolder {
    /** Context. */
    private static final ThreadLocal<SecurityContext> CTX = new ThreadLocal<>();

    /**
     * Get security context.
     *
     * @return Security context.
     */
    @Nullable public static SecurityContext get() {
        return CTX.get();
    }

    /**
     * Set security context.
     *
     * @param ctx Context.
     * @return Old context.
     */
    public static SecurityContext push(@Nullable SecurityContext ctx) {
        SecurityContext oldCtx = CTX.get();

        CTX.set(ctx);

        return oldCtx;
    }

    /**
     * Pop security context.
     *
     * @param oldCtx Old context.
     */
    public static void pop(@Nullable SecurityContext oldCtx) {
        CTX.set(oldCtx);
    }
}

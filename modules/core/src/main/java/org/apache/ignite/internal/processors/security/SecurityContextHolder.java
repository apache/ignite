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
     */
    public static void set(@Nullable SecurityContext ctx) {
        CTX.set(ctx);
    }

    /**
     * Clear security context.
     */
    public static void clear() {
        set(null);
    }
}

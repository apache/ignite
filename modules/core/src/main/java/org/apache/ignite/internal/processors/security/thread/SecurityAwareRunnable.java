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

package org.apache.ignite.internal.processors.security.thread;

import java.util.Map;
import org.apache.ignite.cache.ApplicationContext;
import org.apache.ignite.internal.cache.context.ApplicationContextInternal;
import org.apache.ignite.internal.cache.context.ApplicationContextProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a {@link Runnable} wrapper that executes the original {@link Runnable} with the security context
 * current at the time the wrapper was created.
 */
class SecurityAwareRunnable implements Runnable {
    /** */
    private final Runnable delegate;

    /** */
    private final IgniteSecurity security;

    /** */
    private final @Nullable ApplicationContextProcessor appCtxProc;

    /** */
    private final SecurityContext secCtx;

    /** */
    private final Map<String, String> appAttrs;

    /** */
    private SecurityAwareRunnable(IgniteSecurity security, @Nullable ApplicationContextProcessor appCtxProc, Runnable delegate) {
        assert delegate != null;

        this.delegate = delegate;
        this.security = security;
        this.appCtxProc = appCtxProc;

        secCtx = security.securityContext();

        if (appCtxProc != null) {
            ApplicationContext appCtx = appCtxProc.applicationContext();

            appAttrs = appCtx == null ? null : appCtx.getAttributes();
        }
        else
            appAttrs = null;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (
            OperationSecurityContext ignored = security.withContext(secCtx);
            ApplicationContextInternal ignored0 = appCtxProc == null ? null : appCtxProc.applicationContext(appAttrs)
        ) {
            delegate.run();
        }
    }

    /** */
    static Runnable of(IgniteSecurity security, Runnable delegate) {
        return of(security, null, delegate);
    }

    /** */
    static Runnable of(IgniteSecurity security, ApplicationContextProcessor appCtxProc, Runnable delegate) {
        if (delegate == null || (security.isDefaultContext() && appCtxProc != null && appCtxProc.applicationContext() == null))
            return delegate;

        return new SecurityAwareRunnable(security, appCtxProc, delegate);
    }
}

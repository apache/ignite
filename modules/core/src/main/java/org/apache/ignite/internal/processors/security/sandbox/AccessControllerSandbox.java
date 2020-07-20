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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.internal.processors.security.SecurityUtils.hasSecurityManager;

/**
 * Sandbox that based on AccessController.
 */
public class AccessControllerSandbox implements IgniteSandbox {
    /** Instance of IgniteSecurity. */
    private final IgniteSecurity security;

    /** Logger. */
    private final IgniteLogger log;

    /** Constructor. */
    public AccessControllerSandbox(GridKernalContext ctx, IgniteSecurity security) {
        this.security = security;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public <T> T execute(Callable<T> c) throws IgniteException {
        Objects.requireNonNull(c);

        if (!hasSecurityManager())
            throw new SecurityException("SecurityManager was, but it disappeared!");

        final SecurityContext secCtx = security.securityContext();

        assert secCtx != null;

        final AccessControlContext acc = AccessController.doPrivileged(
            (PrivilegedAction<AccessControlContext>)() -> new AccessControlContext(AccessController.getContext(),
                new IgniteDomainCombiner(secCtx.subject().sandboxPermissions()))
        );

        if (log.isDebugEnabled())
            log.debug("Executing the action inside the sandbox [subjId=" + secCtx.subject().id() + ']');

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>)c::call, acc);
        }
        catch (PrivilegedActionException e) {
            throw new IgniteException(e.getException());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }
}

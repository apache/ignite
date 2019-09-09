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
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.sandbox.closure.SandboxAwareComputeJob;
import org.apache.ignite.internal.processors.security.sandbox.closure.SandboxAwareEntryProcessor;
import org.apache.ignite.internal.processors.security.sandbox.closure.SandboxAwareIgniteBiPredicate;
import org.apache.ignite.internal.processors.security.sandbox.closure.SandboxAwareIgniteClosure;
import org.apache.ignite.internal.processors.security.sandbox.closure.SandboxAwareStreamReceiver;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.stream.StreamReceiver;

/**
 * Sandbox that based on AccessController.
 */
public class AccessControllerSandbox implements IgniteSandbox {
    /** */
    private static final ProtectionDomain[] NULL_PD_ARRAY = new ProtectionDomain[0];

    /** Instance of IgniteSecurity. */
    private final IgniteSecurity security;

    /** Ctor. */
    public AccessControllerSandbox(IgniteSecurity security) {
        this.security = security;
    }

    /** {@inheritDoc} */
    @Override public <T> T execute(Callable<T> call) throws IgniteException {
        Objects.requireNonNull(call);

        if (System.getSecurityManager() == null)
            throw new SecurityException("SecurityManager was, but it disappeared!");

        final SecurityContext secCtx = security.securityContext();

        assert secCtx != null;

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>)call::call,
                new AccessControlContext(
                    new ProtectionDomain[] {
                        new ProtectionDomain(null, secCtx.subject().sandboxPermissions())
                    }
                ));
        }
        catch (PrivilegedActionException pae) {
            throw new IgniteException(pae.getException());
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJob wrapper(ComputeJob job) {
        return job != null ? new SandboxAwareComputeJob(this, job) : job;
    }

    /** {@inheritDoc} */
    @Override public <K, V, T> EntryProcessor<K, V, T> wrapper(EntryProcessor<K, V, T> prc) {
        return prc != null ? new SandboxAwareEntryProcessor<>(this, prc) : prc;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteBiPredicate<K, V> wrapper(IgniteBiPredicate<K, V> p) {
        return p != null ? new SandboxAwareIgniteBiPredicate<>(this, p) : p;
    }

    /** {@inheritDoc} */
    @Override public <E, R> IgniteClosure<E, R> wrapper(IgniteClosure<E, R> c) {
        return c != null ? new SandboxAwareIgniteClosure<>(this, c) : c;
    }

    /** {@inheritDoc} */
    @Override public <K, V> StreamReceiver<K, V> wrapper(StreamReceiver<K, V> r) {
        return r != null ? new SandboxAwareStreamReceiver<>(this, r) : r;
    }
}

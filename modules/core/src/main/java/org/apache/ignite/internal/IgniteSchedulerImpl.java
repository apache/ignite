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

import java.io.Closeable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.NotNull;

/**
 * {@link IgniteScheduler} implementation.
 */
public class IgniteSchedulerImpl implements IgniteScheduler, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteSchedulerImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     */
    public IgniteSchedulerImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> runLocal(@NotNull Runnable r) {
        A.notNull(r, "r");

        guard();

        try {
            return new IgniteFutureImpl<>(ctx.closure().runLocalSafe(localSecureRunnable(r), false));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Closeable runLocal(@NotNull Runnable r, long delay, TimeUnit timeUnit) {
        A.notNull(r, "r");
        A.ensure(delay > 0, "Illegal delay");

        guard();

        try {
            return ctx.timeout().schedule(localSecureRunnable(r), timeUnit.toMillis(delay), -1);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> callLocal(@NotNull Callable<R> c) {
        A.notNull(c, "c");

        guard();

        try {
            return new IgniteFutureImpl<>(ctx.closure().callLocalSafe(localSecureCallable(c), false));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> scheduleLocal(@NotNull Runnable job, String ptrn) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.schedule().schedule(localSecureRunnable(job), ptrn);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> scheduleLocal(@NotNull Callable<R> job, String ptrn) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.schedule().schedule(localSecureCallable(job), ptrn);
        }
        finally {
            unguard();
        }
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        ctx.gateway().readUnlock();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridKernalContext)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return ctx.grid().scheduler();
    }

    /** @return Security aware runnable. */
    private Runnable localSecureRunnable(Runnable original) {
        if (!ctx.security().enabled() || ctx.security().isDefaultContext())
            return original;

        return new SecurityAwareClosure<Void>(ctx.security().securityContext().subject().id(), original);
    }

    /** @return Security aware callable. */
    private <T> Callable<T> localSecureCallable(Callable<T> original) {
        if (!ctx.security().enabled() || ctx.security().isDefaultContext())
            return original;

        return new SecurityAwareClosure<>(ctx.security().securityContext().subject().id(), original);
    }

    /** */
    private class SecurityAwareClosure<T> implements GridPlainRunnable, GridPlainCallable<T>, GridInternalWrapper<Object> {
        /** Security subject id. */
        private final UUID secSubjId;

        /** Runnable. */
        private final Runnable runnable;

        /** Callable. */
        private final Callable<T> call;

        /** */
        private SecurityAwareClosure(UUID secSubjId, Runnable r) {
            this.secSubjId = secSubjId;
            runnable = SecurityUtils.sandboxedProxy(ctx, Runnable.class, r);
            call = null;
        }

        /** */
        private SecurityAwareClosure(UUID secSubjId, Callable<T> c) {
            this.secSubjId = secSubjId;
            call = SecurityUtils.sandboxedProxy(ctx, Callable.class, c);
            runnable = null;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            assert runnable != null;

            try (OperationSecurityContext c = ctx.security().withContext(secSubjId)) {
                runnable.run();
            }
        }

        /** {@inheritDoc} */
        @Override public T call() throws Exception {
            assert call != null;

            try (OperationSecurityContext c = ctx.security().withContext(secSubjId)) {
                return call.call();
            }
        }

        /** {@inheritDoc} */
        @Override public Object userObject() {
            return runnable != null ? runnable : call;
        }
    }
}

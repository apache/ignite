/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.io.Closeable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;

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
    @Override public IgniteFuture<?> runLocal(Runnable r) {
        A.notNull(r, "r");

        guard();

        try {
            return new IgniteFutureImpl<>(ctx.closure().runLocalSafe(r, false));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Closeable runLocal(@Nullable Runnable r, long delay, TimeUnit timeUnit) {
        A.notNull(r, "r");
        A.ensure(delay > 0, "Illegal delay");

        guard();

        try {
            return ctx.timeout().schedule(r, timeUnit.toMillis(delay), -1);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> callLocal(Callable<R> c) {
        A.notNull(c, "c");

        guard();

        try {
            return new IgniteFutureImpl<>(ctx.closure().callLocalSafe(c, false));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> scheduleLocal(Runnable job, String ptrn) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.schedule().schedule(job, ptrn);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> scheduleLocal(Callable<R> job, String ptrn) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.schedule().schedule(job, ptrn);
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
}
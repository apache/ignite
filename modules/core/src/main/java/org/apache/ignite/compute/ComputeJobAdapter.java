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

package org.apache.ignite.compute;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience adapter for {@link ComputeJob} implementations. It provides the
 * following functionality:
 * <ul>
 * <li>
 *      Default implementation of {@link ComputeJob#cancel()} method and ability
 *      to check whether cancellation occurred with {@link #isCancelled()} method.
 * </li>
 * <li>
 *      Ability to set and get job arguments via {@link #setArguments(Object...)}
 *      and {@link #argument(int)} methods.
 * </li>
 * </ul>
 */
public abstract class ComputeJobAdapter implements ComputeJob, Callable<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job argument. */
    private Object[] args;

    /** Cancellation flag. */
    private transient volatile boolean cancelled;

    /**
     * No-arg constructor.
     */
    protected ComputeJobAdapter() {
        /* No-op. */
    }

    /**
     * Creates job with one arguments. This constructor exists for better
     * backward compatibility with internal Ignite 2.x code.
     *
     * @param arg Job argument.
     */
    protected ComputeJobAdapter(@Nullable Object arg) {
        args = new Object[]{arg};
    }

    /**
     * Creates job with specified arguments.
     *
     * @param args Optional job arguments.
     */
    protected ComputeJobAdapter(@Nullable Object... args) {
        this.args = args;
    }

    /**
     * Sets given arguments.
     *
     * @param args Optional job arguments to set.
     */
    public void setArguments(@Nullable Object... args) {
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        cancelled = true;
    }

    /**
     * This method tests whether or not this job was cancelled. This method
     * is thread-safe and can be called without extra synchronization.
     * <p>
     * This method can be periodically called in {@link ComputeJob#execute()} method
     * implementation to check whether or not this job cancelled. Note that system
     * calls {@link #cancel()} method only as a hint and this is a responsibility of
     * the implementation of the job to properly cancel its execution.
     *
     * @return {@code true} if this job was cancelled, {@code false} otherwise.
     */
    protected final boolean isCancelled() {
        return cancelled;
    }

    /**
     * Gets job argument.
     *
     * @param idx Index of the argument.
     * @param <T> Type of the argument to return.
     * @return Job argument.
     * @throws NullPointerException Thrown in case when there no arguments set.
     * @throws IllegalArgumentException Thrown if index is invalid.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <T> T argument(int idx) {
        A.notNull(args, "args");
        A.ensure(idx >= 0 && idx < args.length, "idx >= 0 && idx < args.length");

        return (T)args[idx];
    }

    /**
     * Gets array of job arguments. Note that changes to this array may
     * affect job execution.
     *
     * @return Array of job arguments.
     */
    @Nullable protected Object[] arguments() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public final Object call() {
        return execute();
    }
}
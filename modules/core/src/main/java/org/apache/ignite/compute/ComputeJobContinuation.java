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

import org.jetbrains.annotations.Nullable;

/**
 * Defines continuation support for grid job context.
 */
public interface ComputeJobContinuation {
    /**
     * Checks if job execution has been temporarily held (suspended).
     * <p>
     * If job has completed its execution, then {@code false} is always returned.
     *
     * @return {@code True} if job has been held.
     */
    public boolean heldcc();

    /**
     * Holds (suspends) a given job indefinitely until {@link #callcc()} is called.
     * Job will remain in active queue, but its {@link #heldcc()} method will
     * return {@code true}. Implementations of {@link org.apache.ignite.spi.collision.CollisionSpi} should check
     * if jobs are held or not as needed.
     * <p>
     * All jobs should stop their execution and return right after calling
     * {@code 'holdcc(..)'} method. For convenience, this method always returns {@code null},
     * so you can hold and return in one line by calling {@code 'return holdcc()'} method.
     * <p>
     * If job is not running or has completed its execution, then no-op.
     * <p>
     * The {@code 'cc'} suffix stands for {@code 'current-continuation'} which is a
     * pretty standard notation for this concept that originated from Scheme programming
     * language. Basically, the job is held to be continued later, hence the name of the method.
     *
     * @return Always returns {@code null} for convenience to be used in code with return statement.
     * @throws IllegalStateException If job has been already held before.
     */
    @Nullable public <T> T holdcc();

    /**
     * Holds (suspends) a given job for specified timeout or until {@link #callcc()} is called.
     * Holds (suspends) a given job for specified timeout or until {@link #callcc()} is called.
     * Job will remain in active queue, but its {@link #heldcc()} method will
     * return {@code true}. Implementations of {@link org.apache.ignite.spi.collision.CollisionSpi} should check
     * if jobs are held or not as needed.
     * <p>
     * All jobs should stop their execution and return right after calling
     * {@code 'holdcc(..)'} method. For convenience, this method always returns {@code null},
     * so you can hold and return in one line by calling {@code 'return holdcc()'}.
     * <p>
     * If job is not running or has completed its execution, then no-op.
     * <p>
     * The {@code 'cc'} suffix stands for {@code 'current-continuation'} which is a
     * fairly standard notation for this concept. Basically, the job is <i>held</i> to
     * be continued later, hence the name of the method.
     *
     * @param timeout Timeout in milliseconds after which job will be automatically resumed.
     * @return Always returns {@code null} for convenience to be used in code with return statement.
     * @throws IllegalStateException If job has been already held before
     */
    @Nullable public <T> T holdcc(long timeout);

    /**
     * Resumes job if it was held by {@link #holdcc()} method. Resuming job means that
     * {@link ComputeJob#execute()} method will be called again. It is user's responsibility to check,
     * as needed, whether job is executing from scratch and has been resumed.
     * <p>
     * Note that the job is resumed with exactly the same state as of when it was <i>'held'</i> via
     * the {@link #holdcc()} method.
     * <p>
     * If job is not running, has not been suspended, or has completed its execution, then no-op.
     * <p>
     * The method is named after {@code 'call-with-current-continuation'} design pattern, commonly
     * abbreviated as {@code 'call/cc'}, which originated in Scheme programming language.
     */
    public void callcc();
}
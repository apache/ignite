/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.gridgain.grid.spi.collision.*;
import org.jetbrains.annotations.*;

/**
 * Defines continuation support for grid job context.
 */
public interface GridComputeJobContinuation {
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
     * return {@code true}. Implementations of {@link GridCollisionSpi} should check
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
     */
    @Nullable public <T> T holdcc();

    /**
     * Holds (suspends) a given job for specified timeout or until {@link #callcc()} is called.
     * Holds (suspends) a given job for specified timeout or until {@link #callcc()} is called.
     * Job will remain in active queue, but its {@link #heldcc()} method will
     * return {@code true}. Implementations of {@link GridCollisionSpi} should check
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
     */
    @Nullable public <T> T holdcc(long timeout);

    /**
     * Resumes job if it was held by {@link #holdcc()} method. Resuming job means that
     * {@link GridComputeJob#execute()} method will be called again. It is user's responsibility to check,
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

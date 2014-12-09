/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

/**
 * Convenience adapter for {@link ComputeJob} implementations. It provides the
 * following functionality:
 * <ul>
 * <li>
 *      Default implementation of {@link ComputeJob#cancel()} method and ability
 *      to check whether cancellation occurred.
 * </li>
 * <li>
 *      Ability to set and get a job arguments via {@link #setArguments(Object...)}
 *      and {@link #argument(int)} methods.
 * </li>
 * </ul>
 * Here is an example of how {@code GridComputeJobAdapter} can be used from task logic
 * to create jobs. The example creates job adapter as anonymous class, but you
 * are free to create a separate class for it.
 * <pre name="code" class="java">
 * public class TestGridTask extends GridComputeTaskSplitAdapter&lt;String, Integer&gt; {
 *     // Used to imitate some logic for the
 *     // sake of this example
 *     private int multiplier = 3;
 *
 *     &#64;Override
 *     protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, final String arg) throws GridException {
 *         List&lt;GridComputeJobAdapter&lt;String&gt;&gt; jobs = new ArrayList&lt;GridComputeJobAdapter&lt;String&gt;&gt;(gridSize);
 *
 *         for (int i = 0; i < gridSize; i++) {
 *             jobs.add(new GridComputeJobAdapter() {
 *                 // Job execution logic.
 *                 public Object execute() throws GridException {
 *                     return multiplier * arg.length();
 *                 }
 *             });
 *        }
 *
 *         return jobs;
 *     }
 *
 *     // Aggregate multiple job results into
 *     // one task result.
 *     public Integer reduce(List&lt;GridComputeJobResult&gt; results) throws GridException {
 *         int sum = 0;
 *
 *         // For the sake of this example, let's sum all results.
 *         for (GridComputeJobResult res : results) {
 *             sum += (Integer)res.getData();
 *         }
 *
 *         return sum;
 *     }
 * }
 * </pre>
 */
public abstract class ComputeJobContinuationAdapter extends ComputeJobAdapter implements
    ComputeJobContinuation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job context. */
    @IgniteJobContextResource
    private transient ComputeJobContext jobCtx;

    /**
     * No-arg constructor.
     */
    protected ComputeJobContinuationAdapter() {
        /* No-op. */
    }

    /**
     * Creates job with one arguments. This constructor exists for better
     * backward compatibility with internal GridGain 2.x code.
     *
     * @param arg Job argument.
     */
    protected ComputeJobContinuationAdapter(@Nullable Object arg) {
        super(arg);
    }

    /**
     * Creates job with specified arguments.
     *
     * @param args Optional job arguments.
     */
    protected ComputeJobContinuationAdapter(@Nullable Object... args) {
        super(args);
    }

    /** {@inheritDoc} */
    @Override public boolean heldcc() {
        return jobCtx != null && jobCtx.heldcc();
    }

    /** {@inheritDoc} */
    @Override public void callcc() {
        if (jobCtx != null)
            jobCtx.callcc();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T holdcc() {
        return jobCtx == null ? null : jobCtx.<T>holdcc();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T holdcc(long timeout) {
        return jobCtx == null ? null : jobCtx.<T>holdcc(timeout);
    }
}

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

import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

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
 * Here is an example of how {@code ComputeJobAdapter} can be used from task logic
 * to create jobs. The example creates job adapter as anonymous class, but you
 * are free to create a separate class for it.
 * <pre name="code" class="java">
 * public class TestGridTask extends ComputeTaskSplitAdapter&lt;String, Integer&gt; {
 *     // Used to imitate some logic for the
 *     // sake of this example
 *     private int multiplier = 3;
 *
 *     &#64;Override
 *     protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, final String arg) throws IgniteCheckedException {
 *         List&lt;ComputeJobAdapter&lt;String&gt;&gt; jobs = new ArrayList&lt;ComputeJobAdapter&lt;String&gt;&gt;(gridSize);
 *
 *         for (int i = 0; i < gridSize; i++) {
 *             jobs.add(new ComputeJobAdapter() {
 *                 // Job execution logic.
 *                 public Object execute() throws IgniteCheckedException {
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
 *     public Integer reduce(List&lt;ComputeJobResult&gt; results) throws IgniteCheckedException {
 *         int sum = 0;
 *
 *         // For the sake of this example, let's sum all results.
 *         for (ComputeJobResult res : results) {
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
    @JobContextResource
    private transient ComputeJobContext jobCtx;

    /**
     * No-arg constructor.
     */
    protected ComputeJobContinuationAdapter() {
        /* No-op. */
    }

    /**
     * Creates job with one arguments. This constructor exists for better
     * backward compatibility with internal Ignite 2.x code.
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
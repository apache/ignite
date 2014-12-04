/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import java.lang.annotation.*;
import java.util.*;

/**
 * This annotation disables caching of task results when attached to {@link GridComputeTask} class
 * being executed. Use it when number of jobs within task grows too big, or jobs themselves
 * are too large to keep in memory throughout task execution. By default all results are cached and passed into
 * {@link GridComputeTask#result(GridComputeJobResult,List) GridComputeTask.result(GridComputeJobResult, List&lt;GridComputeJobResult&gt;)}
 * method or {@link GridComputeTask#reduce(List) GridComputeTask.reduce(List&lt;GridComputeJobResult&gt;)} method.
 * When this annotation is attached to a task class, then this list of job results will always be empty.
 * <p>
 * Note that if this annotation is attached to a task class, then job siblings list is not maintained
 * and always has size of {@code 0}. This is done to make sure that in case if task emits large
 * number of jobs, list of jobs siblings does not grow. This only affects the following methods
 * on {@link GridComputeTaskSession}:
 * <ul>
 * <li>{@link GridComputeTaskSession#getJobSiblings()}</li>
 * <li>{@link GridComputeTaskSession#getJobSibling(org.apache.ignite.lang.IgniteUuid)}</li>
 * <li>{@link GridComputeTaskSession#refreshJobSiblings()}</li>
 * </ul>
 *
 * Use this annotation when job results are too large to hold in memory and can be discarded
 * after being processed in
 * {@link GridComputeTask#result(GridComputeJobResult, List) GridComputeTask.result(GridComputeJobResult, List&lt;GridComputeJobResult&gt;)}
 * method.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridComputeTaskNoResultCache {
    // No-op.
}

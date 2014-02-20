// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import java.lang.annotation.*;

/**
 * Annotation for a callback before job starts execution. This annotation allows to call a method
 * right before the job has started execution. This callback only invoked one time before first job execution.
 * <p>
 * This annotation can be applied to {@link GridComputeJob} instances only. It is
 * invoked on the node that executes the job.
 * <p>
 * Example:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     &#64;GridComputeJobBeforeExecute
 *     public void init() {
 *          ...
 *     }
 *     ...
 * }
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface GridComputeJobBeforeExecute {
    // No-op.
}

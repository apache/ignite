// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.*;

import java.lang.annotation.*;

/**
 * This annotation allows to call a method right after the job has been
 * successfully sent for execution. It is useful to clean up the internal
 * state of the job when it is not immediately needed while maintaining
 * effective memory management.
 * <p>
 * This annotation can be applied to {@link GridComputeJob} instance only. It is invoked
 * on the caller node after the job has been sent to remote node for execution.
 * <p>
 * Example:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     &#64;GridComputeJobAfterSend
 *     public void onJobAfterSend() {
 *          ...
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * Note that by default jobs sent to local node are not marshalled and the same
 * instance of job is reused for execution. In this case methods annotated with
 * {@code GridComputeJobAfterSend} annotation will be invoked after completion of the job.
 * To alter this behavior, set {@link GridConfiguration#isMarshalLocalJobs()} to {@code true}.
 *
 * @author @java.author
 * @version @java.version
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface GridComputeJobAfterSend {
    // No-op.
}

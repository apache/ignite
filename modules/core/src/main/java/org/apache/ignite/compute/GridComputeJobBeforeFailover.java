/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.gridgain.grid.spi.failover.*;

import java.lang.annotation.*;

/**
 * This annotation allows to call a method right before job is submitted to
 * {@link GridFailoverSpi}. In this method job can re-create necessary state that was
 * cleared, for example, in method with {@link GridComputeJobAfterSend} annotation.
 * <p>
 * This annotation can be applied to methods of {@link GridComputeJob} instances only. It is
 * invoked on the caller node after remote execution has failed and before the
 * job gets failed over to another node.
 * <p>
 * Example:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     &#64;GridComputeJobBeforeFailover
 *     public void onJobBeforeFailover() {
 *          ...
 *     }
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface GridComputeJobBeforeFailover {
    // No-op.
}

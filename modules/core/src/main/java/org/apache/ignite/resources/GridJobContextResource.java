/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.resources;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for injection of {@link org.apache.ignite.compute.ComputeJobContext} instance.
 * It can be injected into grid jobs only.
 * <p>
 * Job context can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeJob}</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridJobContextResource
 *      private GridComputeJobContext jobCtx;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private GridComputeJobContext jobCtx;
 *     ...
 *     &#64;GridJobContextResource
 *     public void setJobContext(GridComputeJobContext jobCtx) {
 *          this.jobCtx = jobCtx;
 *     }
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridJobContextResource {
    // No-op.
}

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
 * Annotates a field or a setter method for injection of
 * {@link org.apache.ignite.compute.ComputeTaskContinuousMapper} resource.
 * <p>
 * Task continuous mapper can be injected into {@link org.apache.ignite.compute.ComputeTask} class
 * instance.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridTaskContinuousMapperResource
 *      private GridComputeTaskContinuousMapper mapper;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private GridComputeTaskContinuousMapper mapper;
 *     ...
 *     &#64;GridTaskContinuousMapperResource
 *     public void setMapper(GridComputeTaskContinuousMapper mapper) {
 *          this.mapper = mapper;
 *     }
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface IgniteTaskContinuousMapperResource {
    // No-op.
}

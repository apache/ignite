/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.resources;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for injection of {@link GridLogger}. Grid logger is provided to grid
 * via {@link org.apache.ignite.configuration.IgniteConfiguration}.
 * <p>
 * Logger can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.GridComputeTask}</li>
 * <li>{@link org.apache.ignite.compute.GridComputeJob}</li>
 * <li>{@link GridSpi}</li>
 * <li>{@link org.apache.ignite.lifecycle.LifecycleBean}</li>
 * <li>{@link GridUserResource @GridUserResource}</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridLoggerResource
 *      private GridLogger log;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private GridLogger log;
 *     ...
 *     &#64;GridLoggerResource
 *     public void setGridLogger(GridLogger log) {
 *          this.log = log;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See {@link org.apache.ignite.configuration.IgniteConfiguration#getGridLogger()} for Grid configuration details.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridLoggerResource {
    /**
     * Optional log category class. If not provided (i.e. by default
     * {@link Void} class is returned), then the category will
     * be the class into which resource is assigned.
     * <p>
     * Either {@code categoryClass} or {@link #categoryName()} can be provided,
     * by not both.
     *
     * @return Category class of the injected logger.
     */
    public Class categoryClass() default Void.class;

    /**
     * Optional log category name. If not provided, then {@link #categoryClass()}
     * value will be used.
     * <p>
     * Either {@code categoryName} or {@link #categoryClass()} can be provided,
     * by not both.
     *
     * @return Category name for the injected logger.
     */
    public String categoryName() default "";
}

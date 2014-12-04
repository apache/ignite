/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.resources;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.spi.*;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for injection of GridGain grid name.
 * GridGain name is provided to grid via {@link org.gridgain.grid.IgniteConfiguration#getGridName()} method.
 * <p>
 * Grid name can be injected into instances of following classes:
 * <ul>
 * <li>{@link GridComputeTask}</li>
 * <li>{@link GridComputeJob}</li>
 * <li>{@link GridSpi}</li>
 * <li>{@link GridLifecycleBean}</li>
 * <li>{@link GridUserResource @GridUserResource}</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridNameResource
 *      private String name;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private String gridName;
 *     ...
 *     &#64;GridNameResource
 *     public void setGridName(String gridName) {
 *          this.gridName = gridName;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See {@link org.gridgain.grid.IgniteConfiguration#getGridName()} for Grid configuration details.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridNameResource {
    // No-op.
}

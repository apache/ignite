/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.resources;

import java.lang.annotation.*;
import java.util.*;

/**
 * Annotates a field or a setter method for injection of GridGain service(s) by specified service name.
 * If you want to get all services associated with specified service name than use {@link Collection} type
 * (note that no descendant classes are allowed).
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridServiceResource(serviceName = "myService")
 *      private MyService srvc;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private String srvcs;
 *     ...
 *     &#64;GridServiceResource(serviceName = "myService")
 *     public void setGridServices(Collection&lt;MyService&gt; srvcs) {
 *          this.srvcs = srvcs;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridServiceResource {
    /**
     * Service name.
     *
     * @return Name of the injected services.
     */
    public String serviceName();
}

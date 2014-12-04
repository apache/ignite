/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.resources;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for injection of GridGain service(s) by specified service name.
 * If more than one service is deployed on a server, then the first available instance will be returned.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      // Inject single instance of 'myService'. If there is
 *      // more than one, first deployed instance will be picked.
 *      &#64;GridServiceResource(serviceName = "myService", proxyInterface = MyService.class)
 *      private MyService svc;
 *      ...
 *  }
 * </pre>
 * or attach the same annotations to methods:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private MyService svc;
 *     ...
 *      // Inject all locally deployed instances of 'myService'.
 *     &#64;GridServiceResource(serviceName = "myService")
 *     public void setMyService(MyService svc) {
 *          this.svc = svc;
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

    /**
     * In case if an instance of the service is not available locally,
     * an instance of the service proxy for a remote service instance
     * may be returned. If you wish to return only locally deployed
     * instance, then leave this property as {@code null}.
     * <p>
     * For more information about service proxies, see
     * {@link org.apache.ignite.IgniteManaged#serviceProxy(String, Class, boolean)} documentation.
     *
     * @return Interface class for remote service proxy.
     */
    public Class<?> proxyInterface() default Void.class;

    /**
     * Flag indicating if a sticky instance of a service proxy should be returned.
     * This flag is only valid if {@link #proxyInterface()} is not {@code null}.
     * <p>
     * For information about sticky flag, see {@link org.apache.ignite.IgniteManaged#serviceProxy(String, Class, boolean)}
     * documentation.
     *
     * @return {@code True} if a sticky instance of a service proxy should be injected.
     */
    public boolean proxySticky() default false;
}

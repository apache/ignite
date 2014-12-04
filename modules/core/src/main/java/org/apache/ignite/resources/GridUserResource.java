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
 * Annotates a field or a setter method for any custom resources injection.
 * It can be injected into grid tasks and grid jobs. Use it when you would
 * like, for example, to inject something like JDBC connection pool into tasks
 * or jobs - this way your connection pool will be instantiated only once
 * per task and reused for all executions of this task.
 * <p>
 * You can inject other resources into your user resource.
 * The following grid resources can be injected:
 * <ul>
 * <li>{@link IgniteLoggerResource}</li>
 * <li>{@link IgniteLocalNodeIdResource}</li>
 * <li>{@link IgniteHomeResource}</li>
 * <li>{@link GridNameResource}</li>
 * <li>{@link GridMBeanServerResource}</li>
 * <li>{@link IgniteExecutorServiceResource}</li>
 * <li>{@link GridMarshallerResource}</li>
 * <li>{@link GridSpringApplicationContextResource}</li>
 * <li>{@link GridSpringResource}</li>
 * <li>{@link IgniteInstanceResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">Resource Class</h1>
 * The resource will be created based on the {@link #resourceClass()} value. If
 * If {@code resourceClass} is not specified, then field type or setter parameter
 * type will be used to infer the class type of the resource. Set {@link #resourceClass()}
 * to a specific value if the class of resource cannot be inferred from field or setter
 * declaration (for example, if field is an interface).
 * <p>
 * <h1 class="header">Resource Life Cycle</h1>
 * User resource will be instantiated once on every node where task is deployed.
 * Basically there will always be only one instance of resource on any
 * grid node for any task class. Every node will instantiate
 * it's own copy of user resources used for every deployed task (see
 * {@link GridUserResourceOnDeployed} and {@link GridUserResourceOnUndeployed}
 * annotation for resource deployment and undeployment callbacks). For this
 * reason <b>resources should not be sent to remote nodes and should
 * always be declared as transient</b> just in case.
 * <p>
 * Note that an instance of user resource will be created for every deployed task.
 * In case if you need a singleton resource instances on grid nodes (not per-task),
 * you can use {@link GridSpringApplicationContextResource} for injecting per-VM
 * singleton resources configured in Spring.
 * <p>
 * <h1 class="header">Examples</h1>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *      ...
 *      &#64;GridUserResource
 *      private transient MyUserResource rsrc;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private transient MyUserResource rsrc;
 *     ...
 *     &#64;GridUserResource
 *     public void setMyUserResource(MyUserResource rsrc) {
 *          this.rsrc = rsrc;
 *     }
 *     ...
 * }
 * </pre>
 * where resource class can look like this:
 * <pre name="code" class="java">
 * public class MyUserResource {
 *     ...
 *     // Inject logger (or any other resource).
 *     &#64;GridLoggerResource
 *     private GridLogger log;
 *
 *     // Inject grid instance (or any other resource).
 *     &#64;GridInstanceResource
 *     private Grid grid;
 *
 *     // Deployment callback.
 *     &#64;GridUserResourceOnDeployed
 *     public void deploy() {
 *        // Some initialization logic.
 *        ...
 *     }
 *
 *     // Undeployment callback.
 *     &#64;GridUserResourceOnUndeployed
 *     public void undeploy() {
 *        // Some clean up logic.
 *        ...
 *     }
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridUserResource {
    /**
     * Optional resource class. By default the type of the resource variable
     * or setter parameter will be used.
     */
    @SuppressWarnings({"JavaDoc"}) Class<?> resourceClass() default Void.class;

    /**
     * Optional resource name. By default the {@code "dfltName"} values will be used.
     */
    @SuppressWarnings({"JavaDoc"}) String resourceName() default "dfltName";
}

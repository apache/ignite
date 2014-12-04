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
 * Annotates a special methods inside injected user-defined resource {@link GridUserResource}.
 * It can be used in any injected resource for grid tasks and grid jobs. Typically the method with this
 * annotation will be used for initialization of the injectable resource such as opening database connection,
 * network connection or reading configuration settings. Note that this method is called after the resource
 * itself has been injected with all its resources, if any.
 * <p>
 * Here is how annotation would typically happen:
 * <pre name="code" class="java">
 * public class MyUserResource {
 *     ...
 *     &#64;GridLoggerResource
 *     private GridLogger log;
 *
 *     &#64;GridSpringApplicationContextResource
 *     private ApplicationContext springCtx;
 *     ...
 *     &#64;GridUserResourceOnDeployed
 *     private void deploy() {
 *         log.info("Deploying resource: " + this);
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See also {@link GridUserResourceOnUndeployed} for undeployment callbacks.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface GridUserResourceOnDeployed {
    // No-op.
}

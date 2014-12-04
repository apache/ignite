/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lifecycle;

import org.gridgain.grid.*;

/**
 * A bean that reacts to grid lifecycle events defined in {@link LifecycleEventType}.
 * Use this bean whenever you need to plug some custom logic before or after
 * grid startup and stopping routines.
 * <p>
 * There are four events you can react to:
 * <ul>
 * <li>
 *   {@link LifecycleEventType#BEFORE_GRID_START} invoked before grid startup
 *   routine is initiated. Note that grid is not available during this event,
 *   therefore if you injected a grid instance via {@link org.apache.ignite.resources.IgniteInstanceResource}
 *   annotation, you cannot use it yet.
 * </li>
 * <li>
 *   {@link LifecycleEventType#AFTER_GRID_START} invoked right after grid
 *   has started. At this point, if you injected a grid instance via
 *   {@link org.apache.ignite.resources.IgniteInstanceResource} annotation, you can start using it. Note that
 *   you should not be using {@link org.apache.ignite.Ignition} to get grid instance from
 *   lifecycle bean.
 * </li>
 * <li>
 *   {@link LifecycleEventType#BEFORE_GRID_STOP} invoked right before grid
 *   stop routine is initiated. Grid is still available at this stage, so
 *   if you injected a grid instance via  {@link org.apache.ignite.resources.IgniteInstanceResource} annotation,
 *   you can use it.
 * </li>
 * <li>
 *   {@link LifecycleEventType#AFTER_GRID_STOP} invoked right after grid
 *   has stopped. Note that grid is not available during this event.
 * </li>
 * </ul>
 * <h1 class="header">Resource Injection</h1>
 * Lifecycle beans can be injected using IoC (dependency injection) with
 * grid resources. Both, field and method based injection are supported.
 * The following grid resources can be injected:
 * <ul>
 * <li>{@link org.apache.ignite.resources.IgniteLoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteLocalNodeIdResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteHomeResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMBeanServerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteExecutorServiceResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMarshallerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteInstanceResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">Usage</h1>
 * If you need to tie your application logic into GridGain lifecycle,
 * you can configure lifecycle beans via standard grid configuration, add your
 * application library dependencies into {@code GRIDGAIN_HOME/libs} folder, and
 * simply start {@code GRIDGAIN_HOME/ggstart.{sh|bat}} scripts.
 * <p>
 * <h1 class="header">Configuration</h1>
 * Grid lifecycle beans can be configured programmatically as follows:
 * <pre name="code" class="java">
 * Collection&lt;GridLifecycleBean&gt; lifecycleBeans = new ArrayList&lt;GridLifecycleBean&gt;();
 *
 * Collections.addAll(lifecycleBeans, new FooBarLifecycleBean1(), new FooBarLifecycleBean2());
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * cfg.setLifecycleBeans(lifecycleBeans);
 *
 * // Start grid with given configuration.
 * G.start(cfg);
 * </pre>
 * or from Spring XML configuration file as follows:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration"&gt;
 *    ...
 *    &lt;property name="lifecycleBeans"&gt;
 *       &lt;list&gt;
 *          &lt;bean class="foo.bar.FooBarLifecycleBean1"/&gt;
 *          &lt;bean class="foo.bar.FooBarLifecycleBean2"/&gt;
 *       &lt;/list&gt;
 *    &lt;/property&gt;
 *    ...
 * &lt;/bean&gt;
 * </pre>
 */
public interface LifecycleBean {
    /**
     * This method is called when lifecycle event occurs.
     *
     * @param evt Lifecycle event.
     * @throws org.gridgain.grid.GridException Thrown in case of any errors.
     */
    public void onLifecycleEvent(LifecycleEventType evt) throws GridException;
}

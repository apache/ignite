/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.lifecycle;

import org.apache.ignite.IgniteException;

/**
 * A bean that reacts to node lifecycle events defined in {@link LifecycleEventType}.
 * Use this bean whenever you need to plug some custom logic before or after
 * node startup and stopping routines.
 * <p>
 * There are four events you can react to:
 * <ul>
 * <li>
 *   {@link LifecycleEventType#BEFORE_NODE_START} invoked before node startup
 *   routine is initiated. Note that node is not available during this event,
 *   therefore if you injected a ignite instance via {@link org.apache.ignite.resources.IgniteInstanceResource}
 *   annotation, you cannot use it yet.
 * </li>
 * <li>
 *   {@link LifecycleEventType#AFTER_NODE_START} invoked right after node
 *   has started. At this point, if you injected a node instance via
 *   {@link org.apache.ignite.resources.IgniteInstanceResource} annotation, you can start using it. Note that
 *   you should not be using {@link org.apache.ignite.Ignition} to get node instance from
 *   lifecycle bean.
 * </li>
 * <li>
 *   {@link LifecycleEventType#BEFORE_NODE_STOP} invoked right before node
 *   stop routine is initiated. Node is still available at this stage, so
 *   if you injected a ignite instance via  {@link org.apache.ignite.resources.IgniteInstanceResource} annotation,
 *   you can use it.
 * </li>
 * <li>
 *   {@link LifecycleEventType#AFTER_NODE_STOP} invoked right after node
 *   has stopped. Note that node is not available during this event.
 * </li>
 * </ul>
 * <h1 class="header">Resource Injection</h1>
 * Lifecycle beans can be injected using IoC (dependency injection) with
 * ignite resources. Both, field and method based injection are supported.
 * The following ignite resources can be injected:
 * <ul>
 * <li>{@link org.apache.ignite.resources.LoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteInstanceResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">Usage</h1>
 * If you need to tie your application logic into Ignition lifecycle,
 * you can configure lifecycle beans via standard node configuration, add your
 * application library dependencies into {@code IGNITE_HOME/libs} folder, and
 * simply start {@code IGNITE_HOME/ignite.{sh|bat}} scripts.
 * <p>
 * <h1 class="header">Configuration</h1>
 * Node lifecycle beans can be configured programmatically as follows:
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * cfg.setLifecycleBeans(new FooBarLifecycleBean1(), new FooBarLifecycleBean2());
 *
 * // Start grid with given configuration.
 * Ignition.start(cfg);
 * </pre>
 * or from Spring XML configuration file as follows:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
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
     * @throws IgniteException Thrown in case of any errors.
     */
    public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException;
}
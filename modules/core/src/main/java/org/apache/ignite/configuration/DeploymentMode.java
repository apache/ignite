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

package org.apache.ignite.configuration;

import org.jetbrains.annotations.Nullable;

/**
 * Grid deployment mode. Deployment mode is specified at grid startup via
 * {@link org.apache.ignite.configuration.IgniteConfiguration#getDeploymentMode()} configuration property
 * (it can also be specified in Spring XML configuration file). The main
 * difference between all deployment modes is how classes are loaded on remote nodes via peer-class-loading mechanism.
 * <p>
 * The following deployment modes are supported:
 * <ul>
 * <li>{@link #PRIVATE}</li>
 * <li>{@link #ISOLATED}</li>
 * <li>{@link #SHARED}</li>
 * <li>{@link #CONTINUOUS}</li>
 * </ul>
 * <h1 class="header">User Version</h1>
 * User version comes into play whenever you would like to redeploy tasks deployed
 * in {@link #SHARED} or {@link #CONTINUOUS} modes. By default, Ignite will
 * automatically detect if class-loader changed or a node is restarted. However,
 * if you would like to change and redeploy code on a subset of nodes, or in
 * case of {@link #CONTINUOUS} mode to kill the ever living deployment, you should
 * change the user version.
 * <p>
 * User version is specified in {@code META-INF/ignite.xml} file as follows:
 * <pre name="code" class="xml">
 *    &lt;!-- User version. --&gt;
 *    &lt;bean id="userVersion" class="java.lang.String"&gt;
 *        &lt;constructor-arg value="0"/&gt;
 *    &lt;/bean>
 * </pre>
 * By default, all ignite startup scripts ({@code ignite.sh} or {@code ignite.bat})
 * pick up user version from {@code IGNITE_HOME/config/userversion} folder. Usually, it
 * is just enough to update user version under that folder, however, in case of {@code GAR}
 * or {@code JAR} deployment, you should remember to provide {@code META-INF/ignite.xml}
 * file with desired user version in it.
 * <p>
 * <h1 class="header">Always-Local Development</h1>
 * Ignite deployment (regardless of mode) allows you to develop everything as you would
 * locally. You never need to specifically write any kind of code for remote nodes. For
 * example, if you need to use a distributed cache from your {@link org.apache.ignite.compute.ComputeJob}, then you can
 * the following:
 * <ol>
 *  <li>
 *      Simply startup stand-alone Ignite nodes by executing
 *      {@code IGNITE_HOME/ignite.{sh|bat}} scripts.
 *  </li>
 *  <li>
 *      Now, all jobs executing locally or remotely can have a single instance of cache
 *      on every node, and all jobs can access instances stored by any other job without
 *      any need for explicit deployment.
 *  </li>
 * </ol>
 */
public enum DeploymentMode {
    /**
     * In this mode deployed classes do not share resources. Basically, resources are created
     * once per deployed task class and then get reused for all executions.
     * <p>
     * Note that classes deployed within the same class loader on master
     * node, will still share the same class loader remotely on worker nodes.
     * However, tasks deployed from different master nodes will not
     * share the same class loader on worker nodes, which is useful in
     * development when different developers can be working on different
     * versions of the same classes.
     * <p>
     * Also note that resources are associated with task deployment,
     * not task execution. If the same deployed task gets executed multiple
     * times, then it will keep reusing the same user resources
     * every time.
     */
    PRIVATE,

    /**
     * Unlike {@link #PRIVATE} mode, where different deployed tasks will
     * never use the same instance of resources, in {@code ISOLATED}
     * mode, tasks or classes deployed within the same class loader
     * will share the same instances of resources.
     * This means that if multiple tasks classes are loaded by the same
     * class loader on master node, then they will share instances
     * of resources on worker nodes. In other words, user resources
     * get initialized once per class loader and then get reused for all
     * consecutive executions.
     * <p>
     * Note that classes deployed within the same class loader on master
     * node, will still share the same class loader remotely on worker nodes.
     * However, tasks deployed from different master nodes will not
     * share the same class loader on worker nodes, which is especially
     * useful when different developers can be working on different versions
     * of the same classes.
     */
    ISOLATED,

    /**
     * Same as {@link #ISOLATED}, but now tasks from
     * different master nodes with the same user version and same
     * class loader will share the same class loader on remote
     * nodes. Classes will be undeployed whenever all master
     * nodes leave grid or user version changes.
     * <p>
     * The advantage of this approach is that it allows tasks coming from
     * different master nodes share the same instances of resources on worker nodes. This allows for all
     * tasks executing on remote nodes to reuse, for example, the same instances of
     * connection pools or caches. When using this mode, you can
     * startup multiple stand-alone Ignite worker nodes, define resources
     * on master nodes and have them initialize once on worker nodes regardless
     * of which master node they came from.
     * <p>
     * This method is specifically useful in production as, in comparison
     * to {@link #ISOLATED} deployment mode, which has a scope of single
     * class loader on a single master node, this mode broadens the
     * deployment scope to all master nodes.
     * <p>
     * Note that classes deployed in this mode will be undeployed if
     * all master nodes left grid or if user version changed. User version can
     * be specified in {@code META-INF/ignite.xml} file as a Spring bean
     * property with name {@code userVersion}. This file has to be in the class
     * path of the class used for task execution.
     * <p>
     * {@code SHARED} deployment mode is default mode used by the grid.
     */
    SHARED,

    /**
     * Same as {@link #SHARED} deployment mode, but resources will not be undeployed even after all master
     * nodes left grid. Tasks from different master nodes with the same user
     * version and same class loader will share the same class loader on remote
     * worker nodes. Classes will be undeployed whenever user version changes.
     * <p>
     * The advantage of this approach is that it allows tasks coming from
     * different master nodes share the same instances of resources on worker nodes. This allows for all
     * tasks executing on remote nodes to reuse, for example, the same instances of
     * connection pools or caches. When using this mode, you can
     * startup multiple stand-alone Ignite worker nodes, define resources
     * on master nodes and have them initialize once on worker nodes regardless
     * of which master node they came from.
     * <p>
     * This method is specifically useful in production as, in comparison
     * to {@link #ISOLATED} deployment mode, which has a scope of single
     * class loader on a single master node, <tt>CONTINUOUS</tt> mode broadens
     * the deployment scope to all master nodes.
     * <p>
     * Note that classes deployed in <tt>CONTINUOUS</tt> mode will be undeployed
     * only if user version changes. User version can be specified in
     * {@code META-INF/ignite.xml} file as a Spring bean property with name
     * {@code userVersion}. This file has to be in the class
     * path of the class used for task execution.
     */
    CONTINUOUS;

    /** Enum values. */
    private static final DeploymentMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static DeploymentMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
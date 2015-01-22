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

package org.apache.ignite.managed;

import java.io.*;

/**
 * An instance of grid-managed service. Grid-managed services may be deployed from
 * {@link org.apache.ignite.IgniteManaged} facade or directly from grid configuration at startup.
 * <h1 class="header">Deployment</h1>
 * Whenever service is deployed, GridGain will automatically calculate how many
 * instances of this service should be deployed on each node within the cluster.
 * Whenever service is deployed on a cluster node, GridGain will call
 * {@link #execute(ManagedServiceContext)} method on that service. It is up to the user
 * to control whenever the service should exit from the {@code execute} method.
 * For example, user may choose to implement service as follows:
 * <pre name="code" class="java">
 * public class MyGridService implements GridService {
 *      ...
 *      // Example of ignite resource injection. All resources are optional.
 *      // You should inject resources only as needed.
 *      &#64;IgniteInstanceResource
 *      private Grid grid;
 *      ...
 *      &#64;Override public void cancel(GridServiceContext ctx) {
 *          // No-op.
 *      }
 *
 *      &#64;Override public void execute(GridServiceContext ctx) {
 *          // Loop until service is cancelled.
 *          while (!ctx.isCancelled()) {
 *              // Do something.
 *              ...
 *          }
 *      }
 *  }
 * </pre>
 * Consecutively, this service can be deployed as follows:
 * <pre name="code" class="java">
 * ...
 * GridServices svcs = grid.services();
 *
 * GridFuture&lt;?&gt; fut = svcs.deployClusterSingleton("mySingleton", new MyGridService());
 *
 * // Wait for deployment to complete.
 * fut.get();
 * </pre>
 * Or from grid configuration on startup:
 * <pre name="code" class="java">
 * GridConfiguration gridCfg = new GridConfiguration();
 *
 * GridServiceConfiguration svcCfg = new GridServiceConfiguration();
 *
 * // Configuration for cluster-singleton service.
 * svcCfg.setName("mySingleton");
 * svcCfg.setMaxPerNodeCount(1);
 * svcCfg.setTotalCount(1);
 * svcCfg.setService(new MyGridService());
 *
 * gridCfg.setServiceConfiguration(svcCfg);
 * ...
 * GridGain.start(gridCfg);
 * </pre>
 * <h1 class="header">Cancellation</h1>
 * Services can be cancelled by calling any of the {@code cancel} methods on {@link org.apache.ignite.IgniteManaged} API.
 * Whenever a deployed service is cancelled, GridGain will automatically call
 * {@link ManagedService#cancel(ManagedServiceContext)} method on that service.
 * <p>
 * Note that GridGain cannot guarantee that the service exits from {@link ManagedService#execute(ManagedServiceContext)}
 * method whenever {@link #cancel(ManagedServiceContext)} is called. It is up to the user to
 * make sure that the service code properly reacts to cancellations.
 */
public interface ManagedService extends Serializable {
    /**
     * Cancels this service. GridGain will automatically call this method whenever any of the
     * {@code cancel} methods on {@link org.apache.ignite.IgniteManaged} API are called.
     * <p>
     * Note that GridGain cannot guarantee that the service exits from {@link #execute(ManagedServiceContext)}
     * method whenever {@code cancel(GridServiceContext)} method is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     *
     * @param ctx Service execution context.
     */
    public void cancel(ManagedServiceContext ctx);

    /**
     * Pre-initializes service before execution. This method is guaranteed to be called before
     * service deployment is complete (this guarantees that this method will be called
     * before method {@link #execute(ManagedServiceContext)} is called).
     *
     * @param ctx Service execution context.
     * @throws Exception If service initialization failed.
     */
    public void init(ManagedServiceContext ctx) throws Exception;

    /**
     * Starts execution of this service. This method is automatically invoked whenever an instance of the service
     * is deployed on a grid node. Note that service is considered deployed even after it exits the {@code execute}
     * method and can be cancelled (or undeployed) only by calling any of the {@code cancel} methods on
     * {@link org.apache.ignite.IgniteManaged} API. Also note that service is not required to exit from {@code execute} method until
     * {@link #cancel(ManagedServiceContext)} method was called.
     *
     * @param ctx Service execution context.
     * @throws Exception If service execution failed. Not that service will still remain deployed, until
     *      {@link org.apache.ignite.IgniteManaged#cancel(String)} method will be called.
     */
    public void execute(ManagedServiceContext ctx) throws Exception;
}

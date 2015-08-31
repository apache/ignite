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

package org.apache.ignite.services;

import java.io.Serializable;

/**
 * An instance of grid-managed service. Grid-managed services may be deployed from
 * {@link org.apache.ignite.IgniteServices} facade or directly from grid configuration at startup.
 * <h1 class="header">Deployment</h1>
 * Whenever service is deployed, Ignite will automatically calculate how many
 * instances of this service should be deployed on each node within the cluster.
 * Whenever service is deployed on a cluster node, Ignite will call
 * {@link #execute(ServiceContext)} method on that service. It is up to the user
 * to control whenever the service should exit from the {@code execute} method.
 * For example, user may choose to implement service as follows:
 * <pre name="code" class="java">
 * public class MyIgniteService implements Service {
 *      ...
 *      // Example of ignite resource injection. All resources are optional.
 *      // You should inject resources only as needed.
 *      &#64;IgniteInstanceResource
 *      private Ignite ignite;
 *      ...
 *      &#64;Override public void cancel(ServiceContext ctx) {
 *          // No-op.
 *      }
 *
 *      &#64;Override public void execute(ServiceContext ctx) {
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
 * svcs.deployClusterSingleton("mySingleton", new MyGridService());
 * </pre>
 * Or from grid configuration on startup:
 * <pre name="code" class="java">
 * IgniteConfiguration gridCfg = new IgniteConfiguration();
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
 * Ignition.start(gridCfg);
 * </pre>
 * <h1 class="header">Cancellation</h1>
 * Services can be cancelled by calling any of the {@code cancel} methods on {@link org.apache.ignite.IgniteServices} API.
 * Whenever a deployed service is cancelled, Ignite will automatically call
 * {@link Service#cancel(ServiceContext)} method on that service.
 * <p>
 * Note that Ignite cannot guarantee that the service exits from {@link Service#execute(ServiceContext)}
 * method whenever {@link #cancel(ServiceContext)} is called. It is up to the user to
 * make sure that the service code properly reacts to cancellations.
 */
public interface Service extends Serializable {
    /**
     * Cancels this service. Ignite will automatically call this method whenever any of the
     * {@code cancel} methods on {@link org.apache.ignite.IgniteServices} API are called.
     * <p>
     * Note that Ignite cannot guarantee that the service exits from {@link #execute(ServiceContext)}
     * method whenever {@code cancel(GridServiceContext)} method is called. It is up to the user to
     * make sure that the service code properly reacts to cancellations.
     *
     * @param ctx Service execution context.
     */
    public void cancel(ServiceContext ctx);

    /**
     * Pre-initializes service before execution. This method is guaranteed to be called before
     * service deployment is complete (this guarantees that this method will be called
     * before method {@link #execute(ServiceContext)} is called).
     *
     * @param ctx Service execution context.
     * @throws Exception If service initialization failed.
     */
    public void init(ServiceContext ctx) throws Exception;

    /**
     * Starts execution of this service. This method is automatically invoked whenever an instance of the service
     * is deployed on a grid node. Note that service is considered deployed even after it exits the {@code execute}
     * method and can be cancelled (or undeployed) only by calling any of the {@code cancel} methods on
     * {@link org.apache.ignite.IgniteServices} API. Also note that service is not required to exit from {@code execute} method until
     * {@link #cancel(ServiceContext)} method was called.
     *
     * @param ctx Service execution context.
     * @throws Exception If service execution failed. Not that service will still remain deployed, until
     *      {@link org.apache.ignite.IgniteServices#cancel(String)} method will be called.
     */
    public void execute(ServiceContext ctx) throws Exception;
}
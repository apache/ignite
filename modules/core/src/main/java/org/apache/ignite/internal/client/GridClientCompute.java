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

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.jetbrains.annotations.Nullable;

/**
 * A compute projection of grid client. Contains various methods for task execution, full and partial (per node)
 * topology refresh, and log viewing. An initial instance of compute projection over the whole remote grid is
 * provided via {@link GridClient#compute()} method. Further sub-projections may be created via
 * any of the {@code projection(...)} methods on this API.
 * <p>
 * You can create custom client projections based on any user-defined filtering. For example you can create
 * a projection over a certain group of nodes, or over all nodes that have a certain attribute. Once projection
 * is created, only nodes that belong to this projection will be contacted on remote grid for any operation.
 * This essentially allows users to create virtual subgrids for remote task execution.
 * <p>
 * Use any of the {@code execute(...)} methods to execute tasks on remote grid. Note that tasks need
 * to be deployed to remote grid first prior to execution. You can also use any of the
 * {@code affinityExecute(...)} methods to execute tasks on node that have affinity with some data key.
 * This way you can collocate your computation with the data cached on remote nodes.
 * <p>
 * You can also use any of the {@code refreshNode(...)} or {@code refreshTopology(...)} methods
 * to eagerly refresh metrics or attributes on remote nodes (note that attributes are static,
 * so it is sufficient to fetch and cache them only once). Metrics and attributes will be
 * cached in {@link GridClientNode} instances automatically if {@link GridClientConfiguration#isEnableMetricsCache()}
 * or {@link GridClientConfiguration#isEnableAttributesCache()} property is set to {@code true}.
 * <p>
 * Compute client also allows fetching contents of remote log files (including backwards mode) via any of
 * the provided {@code log(...)} methods.
 * <h1 class="header">Affinity Awareness</h1>
 * One of the unique properties of the Ignite remote clients is that they are
 * affinity aware. In other words, both compute and data APIs will optionally
 * contact exactly the node where the data is cached based on some affinity key.
 * This allows for collocation of computations and data and avoids extra network
 * hops that would be necessary if non-affinity nodes were contacted. Use
 * {@link #affinityExecute(String, String, Object, Object)} and
 * {@link #affinityExecuteAsync(String, String, Object, Object)} to synchronously
 * or asynchronously execute remote tasks on affinity nodes based on provided
 * affinity keys.
 */
public interface GridClientCompute {
    /**
     * Creates a projection that will communicate only with specified remote node.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed node is in topology.
     * If any filters were specified in current topology, this method will check if passed node is accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed node is in that subset. If any of the checks fails an exception will be thrown.
     *
     * @param node Single node to which this projection will be restricted.
     * @return Resulting static projection that is bound to a given node.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientNode node) throws GridClientException;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection. If {@code null},
     *      then no filter would be applied to nodes in projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientPredicate<? super GridClientNode> filter) throws GridClientException;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected with balancer of this projection.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted. If {@code null},
     *      created projection is dynamic and will take nodes from topology.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(Collection<GridClientNode> nodes) throws GridClientException;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter. The
     * balancer passed will override default balancer specified in configuration.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection. If {@code null},
     *      then no filter would be applied to nodes in projection.
     * @param balancer Balancer that will select balanced node in resulting projection. If {@code null},
     *      then balancer which was specified while projection construction will be used.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(GridClientPredicate<? super GridClientNode> filter,
        GridClientLoadBalancer balancer) throws GridClientException;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected with passed balancer..
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted. If {@code null},
     *      then no filter would be applied to nodes in projection.
     * @param balancer Balancer that will select nodes in resulting projection. If {@code null},
     *      then balancer which was specified while projection construction will be used.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientCompute projection(Collection<GridClientNode> nodes, GridClientLoadBalancer balancer)
        throws GridClientException;

    /**
     * Gets load balancer used by this projection. By default, the balancer specified
     * in {@link GridClientConfiguration#getBalancer()} property is used. User can
     * provide custom balancers for different projections via
     * {@link #projection(GridClientPredicate, GridClientLoadBalancer)} method.
     *
     * @return Instance of {@link GridClientLoadBalancer} used by this projection.
     */
    public GridClientLoadBalancer balancer();

    /**
     * Executes task on remote grid. Only nodes included into this projection will
     * be contacted. Note that task must be deployed on remote grid prior to the
     * execution.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <R> R execute(String taskName, Object taskArg) throws GridClientException;

    /**
     * Asynchronously executes task on remote grid. Only nodes included into this projection will
     * be contacted. Note that task must be deployed on remote grid prior to the
     * execution.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Future for remote execution.
     */
    public <R> GridClientFuture<R> executeAsync(String taskName, Object taskArg);

    /**
     * Executes task using cache affinity key for routing. This way the task will start executing
     * exactly on the node where this affinity key is cached hence allowing for
     * collocation of computations and data.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.  If {@code null},
     *      then default cache will be used.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <R> R affinityExecute(String taskName, String cacheName, Object affKey, Object taskArg)
        throws GridClientException;

    /**
     * Asynchronously executes task using cache affinity key for routing. This way
     * the task will start executing exactly on the node where this affinity key is cached
     * hence allowing for collocation of computations and data.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.  If {@code null},
     *      then balancer which was specified while projection construction will be used.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Future for the remote execution.
     */
    public <R> GridClientFuture<R> affinityExecuteAsync(String taskName, String cacheName, Object affKey,
        Object taskArg);

    /**
     * Gets most recently refreshed topology (only non-daemon nodes included).
     * If this compute instance is a projection, then only nodes that
     * satisfy projection criteria will be returned.
     *
     * @return Most recently refreshed topology.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public Collection<GridClientNode> nodes() throws GridClientException;

    /**
     * Gets cached node with given id from most recently refreshed topology.
     *
     * @param id Node ID.
     * @return Node for given ID or {@code null} if node with given id was not found.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
     public GridClientNode node(UUID id) throws GridClientException;

    /**
     * Gets cached nodes for the given IDs based on most recently refreshed topology.
     * If this compute instance is a projection, then only nodes that passes projection
     * criteria will be returned.
     *
     * @param ids Node IDs.
     * @return Collection of nodes for provided IDs.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public Collection<GridClientNode> nodes(Collection<UUID> ids) throws GridClientException;

    /**
     * Gets all cached nodes that pass the filter. If this compute instance is a projection, then only
     * nodes that passes projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public Collection<GridClientNode> nodes(GridClientPredicate<GridClientNode> filter)
        throws GridClientException;

    /**
     * Gets most recently refreshed set of daemon nodes. If this compute instance is a projection,
     * then only nodes that satisfy projection criteria will be returned.
     *
     * @return Daemon nodes in most recently refreshed topology.
     * @throws GridClientException If client doesn't have an actual topology version.
     */
    public Collection<GridClientNode> daemonNodes() throws GridClientException;

    /**
     * Refreshes and returns node by its ID from remote grid. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()} parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to true.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or {@code null} if node doesn't exist.
     * @throws GridClientException In case request failed.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientNode refreshNode(UUID id, boolean includeAttrs, boolean includeMetrics) throws GridClientException;

    /**
     * Asynchronously refreshes and returns node by its ID from remote grid. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()} parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to true.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future for the refresh
     */
    public GridClientFuture<GridClientNode> refreshNodeAsync(UUID id, boolean includeAttrs, boolean includeMetrics);

    /**
     * Refreshes and returns node by its IP address from remote grid. All possible IP addresses
     * of a node will be checked. If there is more than one node for given IP address, then
     * first found node will be refreshed. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()} parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to true.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or {@code null} if node doesn't exist.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    @Nullable public GridClientNode refreshNode(String ip, boolean includeAttrs, boolean includeMetrics)
        throws GridClientException;

    /**
     * Asynchronously refreshes and returns node by its IP address from remote grid. All possible IP addresses
     * of a node will be checked. If there is more than one node for given IP address, then
     * first found node will be refreshed. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()} parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to true.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future for the refresh operation.
     */
    public GridClientFuture<GridClientNode> refreshNodeAsync(String ip, boolean includeAttrs, boolean includeMetrics);

    /**
     * Refreshes and returns all nodes within topology. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()} parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to {@code true}.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptors.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public List<GridClientNode> refreshTopology(boolean includeAttrs, boolean includeMetrics)
        throws GridClientException;

    /**
     * Asynchronously refreshes and returns all nodes within topology. Use {@code includeAttrs} and
     * {@code includeMetrics} parameters to automatically fetch remote node attributes and
     * metrics.
     * <p>
     * Note that fetched attributes and metrics may or may note be cached in {@link GridClientNode}
     * based on {@link GridClientConfiguration#isEnableMetricsCache()} and
     * {@link GridClientConfiguration#isEnableAttributesCache()}parameters. Even though topology
     * is refreshed automatically every {@link GridClientConfiguration#getTopologyRefreshFrequency()}
     * interval, node metrics and attributes will be fetched in background only if
     * {@link GridClientConfiguration#isAutoFetchMetrics()} or
     * {@link GridClientConfiguration#isAutoFetchAttributes()} set to {@code true}.
     * <p>
     * Also note that node attributes are static and, if cached, there is no need
     * to refresh them again.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    public GridClientFuture<List<GridClientNode>> refreshTopologyAsync(boolean includeAttrs, boolean includeMetrics);

    /**
     * Sets keep portables flag for the next task execution in the current thread.
     */
    public GridClientCompute withKeepPortables();
}
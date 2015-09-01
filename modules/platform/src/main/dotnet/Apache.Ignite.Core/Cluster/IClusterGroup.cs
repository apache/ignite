/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cluster 
{   
    using System;
    using System.Collections.Generic;

    using GridGain.Compute;
    using GridGain.Events;
    using GridGain.Services;

    /// <summary>
    /// Defines grid projection which represents a common functionality over a group of nodes.
    /// Grid projection allows to group grid nodes into various subgroups to perform distributed
    /// operations on them. All ForXXX(...)' methods will create a child grid projection
    /// from existing projection. If you create a new projection from current one, then the resulting
    /// projection will include a subset of nodes from current projection. The following code snippet
    /// shows how to create grid projections:
    /// <code>
    /// var g = GridFactory.Grid();
    /// 
    /// // Projection over remote nodes.
    /// var remoteNodes = g.ForRemotes();
    /// 
    /// // Projection over random remote node.
    /// var randomNode = g.ForRandom();
    /// 
    /// // Projection over all nodes with cache named "myCache" enabled.
    /// var cacheNodes = g.ForCacheNodes("myCache");
    /// 
    /// // Projection over all nodes that have user attribute "group" set to value "worker".
    /// var workerNodes = g.ForAttribute("group", "worker");
    /// </code>
    /// Grid projection provides functionality for executing tasks and closures over 
    /// nodes in this projection using <see cref="IClusterGroup.Compute()"/>.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IClusterGroup {
        /// <summary>
        /// Instance of grid.
        /// </summary>
        IGrid Grid
        {
            get;
        }

        /// <summary>
        /// Gets compute functionality over this grid projection. All operations
        /// on the returned ICompute instance will only include nodes from
        /// this projection.
        /// </summary>
        /// <returns>Compute instance over this grid projection.</returns>
        ICompute Compute();

        /// <summary>
        /// Creates a grid projection over a given set of nodes.
        /// </summary>
        /// <param name="nodes">Collection of nodes to create a projection from.</param>
        /// <returns>Projection over provided grid nodes.</returns>
        IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes);

        /// <summary>
        /// Creates a grid projection over a given set of nodes.
        /// </summary>
        /// <param name="nodes">Collection of nodes to create a projection from.</param>
        /// <returns>Projection over provided grid nodes.</returns>
        IClusterGroup ForNodes(params IClusterNode[] nodes);

        /// <summary>
        /// Creates a grid projection over a given set of node IDs.
        /// </summary>
        /// <param name="ids">Collection of node IDs to create a projection from.</param>
        /// <returns>Projection over provided grid node IDs.</returns>
        IClusterGroup ForNodeIds(IEnumerable<Guid> ids);

        /// <summary>
        /// Creates a grid projection over a given set of node IDs.
        /// </summary>
        /// <param name="ids">Collection of node IDs to create a projection from.</param>
        /// <returns>Projection over provided grid node IDs.</returns>
        IClusterGroup ForNodeIds(params Guid[] ids);

        /// <summary>
        /// Creates a grid projection which includes all nodes that pass the given predicate filter.
        /// </summary>
        /// <param name="p">Predicate filter for nodes to include into this projection.</param>
        /// <returns>Grid projection for nodes that passed the predicate filter.</returns>
        IClusterGroup ForPredicate(Func<IClusterNode, bool> p);

        /// <summary>
        /// Creates projection for nodes containing given name and value
        /// specified in user attributes.
        /// </summary>
        /// <param name="name">Name of the attribute.</param>
        /// <param name="val">Optional attribute value to match.</param>
        /// <returns>Grid projection for nodes containing specified attribute.</returns>
        IClusterGroup ForAttribute(string name, string val);

        /// <summary>
        /// Creates projection for all nodes that have cache with specified name running.
        /// </summary>
        /// <param name="name">Cache name to include into projection.</param>
        /// <returns>Projection over nodes that have specified cache running.</returns>
        IClusterGroup ForCacheNodes(string name);
        
        /// <summary>
        /// Creates projection for all nodes that have cache with specified name running 
        /// and cache distribution mode is PARTITIONED_ONLY or NEAR_PARTITIONED.
        /// </summary>
        /// <param name="name">Cache name to include into projection.</param>
        /// <returns>Projection over nodes that have specified cache running.</returns>
        IClusterGroup ForDataNodes(string name);
        
        /// <summary>
        /// Creates projection for all nodes that have cache with specified name running 
        /// and cache distribution mode is CLIENT_ONLY or NEAR_ONLY.
        /// </summary>
        /// <param name="name">Cache name to include into projection.</param>
        /// <returns>Projection over nodes that have specified cache running.</returns>
        IClusterGroup ForClientNodes(string name);

        /// <summary>
        /// Gets grid projection consisting from the nodes in this projection excluding the local node.
        /// </summary>
        /// <returns>Grid projection consisting from the nodes in this projection excluding the local node.</returns>
        IClusterGroup ForRemotes();

        /// <summary>
        /// Gets grid projection consisting from the nodes in this projection residing on the
        /// same host as given node.
        /// </summary>
        /// <param name="node">Node residing on the host for which projection is created.</param>
        /// <returns>Projection for nodes residing on the same host as passed in node.</returns>
        IClusterGroup ForHost(IClusterNode node);

        /// <summary>
        /// Creates grid projection with one random node from current projection.
        /// </summary>
        /// <returns>Grid projection with one random node from current projection.</returns>
        IClusterGroup ForRandom();

        /// <summary>
        /// Creates grid projection with one oldest node in the current projection.
        /// The resulting projection is dynamic and will always pick the next oldest
        /// node if the previous one leaves topology even after the projection has
        /// been created.
        /// </summary>
        /// <returns>Grid projection with one oldest node from the current projection.</returns>
        IClusterGroup ForOldest();

        /// <summary>
        /// Creates grid projection with one youngest node in the current projection.
        /// The resulting projection is dynamic and will always pick the newest
        /// node in the topology, even if more nodes entered after the projection
        /// has been created.
        /// </summary>
        /// <returns>Grid projection with one youngest node from the current projection.</returns>
        IClusterGroup ForYoungest();

        /// <summary>
        /// Creates grid projection for nodes supporting .Net, i.e. for nodes started with GridGain.exe.
        /// </summary>
        /// <returns>Grid projection for nodes supporting .Net.</returns>
        IClusterGroup ForDotNet();

        /// <summary>
        /// Gets read-only collections of nodes in this projection.
        /// </summary>
        /// <returns>All nodes in this projection.</returns>
        ICollection<IClusterNode> Nodes();

        /// <summary>
        /// Gets a node for given ID from this grid projection.
        /// </summary>
        /// <param name="id">Node ID.</param>
        /// <returns>Node with given ID from this projection or null if such node does not 
        /// exist in this projection.</returns>
        IClusterNode Node(Guid id);

        /// <summary>
        /// Gets first node from the list of nodes in this projection.
        /// </summary>
        /// <returns>Grid instance.</returns>
        IClusterNode Node();

        /// <summary>
        /// Gets a metrics snapshot for this projection
        /// </summary>
        /// <returns>Grid projection metrics snapshot.</returns>
        IClusterMetrics Metrics();

        /// <summary>
        /// Gets messaging facade over nodes within this cluster group.  All operations on the returned 
        /// <see cref="IMessaging"/>> instance will only include nodes from current cluster group.
        /// </summary>
        /// <returns>Messaging instance over this cluster group.</returns>
        IMessaging Message();

        /// <summary>
        /// Gets events facade over nodes within this cluster group.  All operations on the returned 
        /// <see cref="IEvents"/>> instance will only include nodes from current cluster group.
        /// </summary>
        /// <returns>Events instance over this cluster group.</returns>
        IEvents Events();

        /// <summary>
        /// Gets services facade over nodes within this cluster group.  All operations on the returned 
        /// <see cref="IServices"/>> instance will only include nodes from current cluster group.
        /// </summary>
        /// <returns>Services instance over this cluster group.</returns>
        IServices Services();
    }
}

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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using System.Linq;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Impl.Client.Compute;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client projection implementation.
    /// </summary>
    internal class ClientClusterGroup : IClientClusterGroup
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform: .NET. */
        private const string PlatformDotNet = "dotnet";

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Topology version. */
        private long _topVer;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Current projection. */
        private readonly ClientClusterGroupProjection _projection;

        /** Predicate. */
        private readonly Func<IClientClusterNode, bool> _predicate;

        /** Node ids collection. */
        private Guid[] _nodeIds;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        internal ClientClusterGroup(IgniteClient ignite)
            : this(ignite, ClientClusterGroupProjection.Empty)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="projection">Projection.</param>
        /// <param name="predicate">Predicate.</param>
        private ClientClusterGroup(IgniteClient ignite, 
            ClientClusterGroupProjection projection, Func<IClientClusterNode, bool> predicate = null)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
            _projection = projection;
            _predicate = predicate;
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return new ClientClusterGroup(_ignite, _projection.ForAttribute(name, val));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, PlatformDotNet);
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForServers()
        {
            return new ClientClusterGroup(_ignite, _projection.ForServerNodes(true));
        }

        /** <inheritDoc /> */
        public IClientClusterGroup ForPredicate(Func<IClientClusterNode, bool> p)
        {
            IgniteArgumentCheck.NotNull(p, "p");

            var newPredicate = _predicate == null ? p : node => _predicate(node) && p(node);
            return new ClientClusterGroup(_ignite, _projection, newPredicate);
        }

        /** <inheritDoc /> */
        public ICollection<IClientClusterNode> GetNodes()
        {
            return RefreshNodes();
        }

        /** <inheritDoc /> */
        public IClientClusterNode GetNode(Guid id)
        {
            if (id == Guid.Empty)
            {
                throw new ArgumentException("Node id should not be empty.");
            }

            return GetNodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClientClusterNode GetNode()
        {
            return GetNodes().FirstOrDefault();
        }

        /** <inheritDoc /> */
        public IComputeClient GetCompute()
        {
            return new ComputeClient(_ignite, ComputeClientFlags.None, TimeSpan.Zero, this);
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClientClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            var topology = RequestTopologyInformation(oldTopVer);
            if (topology != null)
            {
                UpdateTopology(topology.Item1, topology.Item2);
                RequestNodesInfo(topology.Item2);
            }

            // No topology changes.
            Debug.Assert(_nodeIds != null, "At least one topology update should have occurred.");

            // Local lookup with a native predicate is a trade off between complexity and consistency.
            var nodesList = new List<IClientClusterNode>();
            foreach (Guid nodeId in _nodeIds)
            {
                IClientClusterNode node = _ignite.GetClientNode(nodeId);
                if (_predicate == null || _predicate(node))
                {
                    nodesList.Add(node);
                }
            }

            return nodesList;
        }

        /// <summary>
        /// Request topology information.
        /// </summary>
        /// <returns>Topology version with nodes identifiers.</returns>rns>
        private Tuple<long, Guid[]> RequestTopologyInformation(long oldTopVer)
        {
            Action<ClientRequestContext> writeAction = ctx =>
            {
                ctx.Stream.WriteLong(oldTopVer);
                _projection.Write(ctx.Writer);
            };

            Func<ClientResponseContext, Tuple<long, Guid[]>> readFunc = ctx =>
            {
                if (!ctx.Stream.ReadBool())
                {
                    // No topology changes.
                    return null;
                }

                long remoteTopVer = ctx.Stream.ReadLong();
                return Tuple.Create(remoteTopVer, ReadNodeIds(ctx.Reader));
            };

            return DoOutInOp(ClientOp.ClusterGroupGetNodeIds, writeAction, readFunc);
        }

        /// <summary>
        /// Reads node ids.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Node ids array.</returns>
        private static Guid[] ReadNodeIds(IBinaryRawReader reader)
        {
            int nodesCount = reader.ReadInt();

            var nodeIds = new Guid[nodesCount];
            var stream = ((BinaryReader) reader).Stream;
            
            for (int i = 0; i < nodesCount; i++)
            {
                nodeIds[i] = BinaryUtils.ReadGuid(stream);
            }

            return nodeIds;
        }

        /// <summary>
        /// Update topology.
        /// </summary>
        /// <param name="remoteTopVer">Remote topology version.</param>
        /// <param name="nodeIds">Node ids.</param>
        internal void UpdateTopology(long remoteTopVer, Guid[] nodeIds)
        {
            lock (_syncRoot)
            {
                // If another thread already advanced topology version further, we still
                // can safely return currently received nodes, but we will not assign them.
                if (_topVer < remoteTopVer)
                {
                    Interlocked.Exchange(ref _topVer, remoteTopVer);
                    _nodeIds = nodeIds;
                }
            }
        }

        /// <summary>
        /// Gets nodes information.
        /// This method will filter only unknown node ids that
        /// have not been serialized inside IgniteClient before.
        /// </summary>
        /// <param name="nodeIds">Node ids array.</param>
        /// <returns>Collection of <see cref="IClusterNode"/> instances.</returns>
        private void RequestNodesInfo(Guid[] nodeIds)
        {
            var unknownNodes = new List<Guid>(nodeIds.Length);
            foreach (var nodeId in nodeIds)
            {
                if (!_ignite.ContainsNode(nodeId))
                {
                    unknownNodes.Add(nodeId);
                }
            }
            
            if (unknownNodes.Count > 0)
            {
                RequestRemoteNodesDetails(unknownNodes);
            }
        }

        /// <summary>
        /// Make remote API call to fetch node information.
        /// </summary>
        /// <param name="ids">Node identifiers.</param>
        private void RequestRemoteNodesDetails(List<Guid> ids)
        {
            Action<ClientRequestContext> writeAction = ctx =>
            {
                ctx.Stream.WriteInt(ids.Count);
                foreach (var id in ids)
                {
                    BinaryUtils.WriteGuid(id, ctx.Stream);
                }
            };
            
            Func<ClientResponseContext, bool> readFunc = ctx =>
            {
                var cnt = ctx.Stream.ReadInt();
                for (var i = 0; i < cnt; i++)
                {
                    _ignite.SaveClientClusterNode(ctx.Reader);
                }

                return true;
            };

            DoOutInOp(ClientOp.ClusterGroupGetNodesInfo, writeAction, readFunc);
        }


        /// <summary>
        /// Does the out in op.
        /// </summary>
        protected T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, writeAction, readFunc, HandleError<T>);
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private static T HandleError<T>(ClientStatusCode status, string msg)
        {
            throw new IgniteClientException(msg, null, status);
        }
    }
}

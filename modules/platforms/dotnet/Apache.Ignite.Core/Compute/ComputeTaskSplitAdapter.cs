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

namespace Apache.Ignite.Core.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Compute;

    /// <summary>
    /// This class defines simplified adapter for <see cref="IComputeTask{A,T,R}"/>. This adapter can be used
    /// when jobs can be randomly assigned to available Ignite nodes. This adapter is sufficient
    /// in most homogeneous environments where all nodes are equally suitable for executing grid
    /// job, see <see cref="Split"/> method for more details.
    /// </summary>
    public abstract class ComputeTaskSplitAdapter<TA, T, TR> : ComputeTaskAdapter<TA, T, TR>
    {
        /** Random generator */
        [ThreadStatic]
        // ReSharper disable once StaticMemberInGenericType
        private static Random _rnd;

        /// <summary>
        /// This is a simplified version of <see cref="IComputeTask{A,T,R}.Map"/> method.
        /// <p/>
        /// This method basically takes given argument and splits it into a collection
        /// of <see cref="IComputeJob"/> using provided grid size as indication of how many node are
        /// available. These jobs will be randomly mapped to available Ignite nodes. Note that
        /// if number of jobs is greater than number of Ignite nodes (i.e, grid size), the grid
        /// nodes will be reused and some jobs will end up on the same Ignite nodes.
        /// </summary>
        /// <param name="gridSize">Number of available Ignite nodes. Note that returned number of jobs can be less, 
        ///  equal or greater than this grid size.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>.</param>
        protected abstract ICollection<IComputeJob<T>> Split(int gridSize, TA arg);

        /// <summary>
        /// This method is called to map or split Ignite task into multiple Ignite jobs. This is the
        /// first method that gets called when task execution starts.
        /// </summary>
        /// <param name="subgrid">Nodes available for this task execution. Note that order of nodes is
        /// guaranteed to be randomized by container. This ensures that every time you simply iterate
        /// through Ignite nodes, the order of nodes will be random which over time should result into
        /// all nodes being used equally.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>. This is the same argument
        /// as the one passed into <c>ICompute.Execute()</c> methods.</param>
        /// <returns>
        /// Map of Ignite jobs assigned to subgrid node. If <c>null</c> or empty map is returned,
        /// exception will be thrown.
        /// </returns>
        /// <exception cref="IgniteException">Split returned no jobs.</exception>
        override public IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, TA arg)
        {
            Debug.Assert(subgrid != null && subgrid.Count > 0);

            var jobs = Split(subgrid.Count, arg);

            if (jobs == null || jobs.Count == 0)
                throw new IgniteException("Split returned no jobs.");

            var map = new Dictionary<IComputeJob<T>, IClusterNode>(jobs.Count);

            if (_rnd == null)
                _rnd = new Random();

            foreach (var job in jobs)
            {
                int idx = _rnd.Next(subgrid.Count);

                IClusterNode node = subgrid[idx];

                map[job] = node;
            }

            return map;
        }
    }
}

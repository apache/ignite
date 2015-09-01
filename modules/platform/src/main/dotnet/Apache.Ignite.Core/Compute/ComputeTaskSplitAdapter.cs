/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    using GridGain.Cluster;
    using GridGain.Common;

    /// <summary>
    /// This class defines simplified adapter for <see cref="IComputeTask{A,T,R}"/>. This adapter can be used
    /// when jobs can be randomly assigned to available grid nodes. This adapter is sufficient
    /// in most homogeneous environments where all nodes are equally suitable for executing grid
    /// job, see <see cref="Split"/> method for more details.
    /// </summary>
    public abstract class ComputeTaskSplitAdapter<A, T, R> : ComputeTaskAdapter<A, T, R>
    {
        /** Random generator */
        [ThreadStatic]
        // ReSharper disable once StaticMemberInGenericType
        private static Random rnd;

        /// <summary>
        /// This is a simplified version of <see cref="IComputeTask{A,T,R}.Map"/> method.
        /// <p/>
        /// This method basically takes given argument and splits it into a collection
        /// of <see cref="GridGain.Compute.IComputeJob"/> using provided grid size as indication of how many node are
        /// available. These jobs will be randomly mapped to available grid nodes. Note that
        /// if number of jobs is greater than number of grid nodes (i.e, grid size), the grid
        /// nodes will be reused and some jobs will end up on the same grid nodes.
        /// </summary>
        /// <param name="gridSize">Number of available grid nodes. Note that returned number of jobs can be less, 
        ///  equal or greater than this grid size.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>.</param>
        protected abstract ICollection<IComputeJob<T>> Split(int gridSize, A arg);

        /// <summary>
        /// This method is called to map or split grid task into multiple grid jobs. This is the
        /// first method that gets called when task execution starts.
        /// </summary>
        /// <param name="subgrid">Nodes available for this task execution. Note that order of nodes is
        /// guaranteed to be randomized by container. This ensures that every time you simply iterate
        /// through grid nodes, the order of nodes will be random which over time should result into
        /// all nodes being used equally.</param>
        /// <param name="arg">Task execution argument. Can be <c>null</c>. This is the same argument
        /// as the one passed into <c>ICompute.Execute()</c> methods.</param>
        /// <returns>
        /// Map of grid jobs assigned to subgrid node. If <c>null</c> or empty map is returned,
        /// exception will be thrown.
        /// </returns>
        /// <exception cref="GridGain.Common.IgniteException">Split returned no jobs.</exception>
        override public IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, A arg)
        {
            Debug.Assert(subgrid != null && subgrid.Count > 0);

            var jobs = Split(subgrid.Count, arg);

            if (jobs == null || jobs.Count == 0)
                throw new IgniteException("Split returned no jobs.");

            var map = new Dictionary<IComputeJob<T>, IClusterNode>(jobs.Count);

            if (rnd == null)
                rnd = new Random();

            foreach (var job in jobs)
            {
                int idx = rnd.Next(subgrid.Count);

                IClusterNode node = subgrid[idx];

                map[job] = node;
            }

            return map;
        }
    }
}

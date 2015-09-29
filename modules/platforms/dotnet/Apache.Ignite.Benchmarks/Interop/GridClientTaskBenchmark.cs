/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Interop
{
    using System.Collections.Generic;
    using GridGain.Cluster;
    using GridGain.Compute;

    /// <summary>
    ///
    /// </summary>
    internal class GridClientTaskBenchmark : GridClientAbstractInteropBenchmark
    {
        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("ExecuteEmptyTask", ExecuteEmptyTask, 1));
        }

        /// <summary>
        /// Executes task.
        /// </summary>
        private void ExecuteEmptyTask(GridClientBenchmarkState state)
        {
            node.Compute().Execute(new MyEmptyTask(), "zzzz");
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class MyEmptyTask : IComputeTask<object, object, object>
    {
        /// <summary>
        ///
        /// </summary>
        /// <param name="subgrid"></param>
        /// <param name="arg"></param>
        /// <returns></returns>
        public IDictionary<IComputeJob<object>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return new Dictionary<IComputeJob<object>, IClusterNode>
            {
                {new MyJob((string) arg), subgrid[0]}
            };
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="res"></param>
        /// <param name="rcvd"></param>
        /// <returns></returns>
        public ComputeJobResultPolicy Result(IComputeJobResult<object> res, IList<IComputeJobResult<object>> rcvd)
        {
            return ComputeJobResultPolicy.WAIT;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="results"></param>
        /// <returns></returns>
        public object Reduce(IList<IComputeJobResult<object>> results)
        {
            return results.Count == 0 ? null : results[0];
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class MyJob : IComputeJob<object>
    {
        /** */
        private readonly string s;

        public MyJob(string s)
        {
            this.s = s;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public object Execute()
        {
            return s.Length;
        }

        /// <summary>
        ///
        /// </summary>
        public void Cancel()
        {
            // No-op.
        }
    }
}

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

    using GridGain.Compute;

    /// <summary>
    /// 
    /// </summary>
    internal class GridClientClosureBenchmark : GridClientAbstractInteropBenchmark
    {
        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("ExecuteClosureTask", ExecuteClosureTask, 1));
        }
        
        /// <summary>
        /// Executes closure.
        /// </summary>
        private void ExecuteClosureTask(GridClientBenchmarkState state)
        {
            node.Compute().Call(new MyClosure("zzzz"));
        }
    }

    public class MyClosure : IComputeFunc<int>
    {
        /** */
        private readonly string s;

        public MyClosure(string s)
        {
            this.s = s;
        }

        public int Invoke()
        {
            return s.Length;
        }
    }
}

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

    using GridGain.Client.Benchmark.Model;
    using GridGain.Cache;

    /// <summary>
    /// 
    /// </summary>
    internal class GridClientGetAsyncInteropBenchmark : GridClientAbstractInteropBenchmark
    {
        /** Cache name. */
        private const string CACHE_NAME = "cache";

        /** Native cache wrapper. */
        private ICache<int, Employee> cache;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            cache = node.Cache<int, Employee>(CACHE_NAME);

            for (int i = 0; i < emps.Length; i++)
                cache.Put(i, emps[i]);

            cache = cache.WithAsync();
        }

        /** <inheritDoc /> */
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("GetAsync", GetAsync, 1));
        }
        
        /// <summary>
        /// Cache getAsync.
        /// </summary>
        private void GetAsync(GridClientBenchmarkState state)
        {
            int idx = GridClientBenchmarkUtils.RandomInt(Dataset);

            cache.Get(idx);

            cache.GetFuture<Employee>().ToTask().Wait();

            //System.Console.WriteLine(res);
        }
    }
}

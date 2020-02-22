namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache put benchmark.
    /// </summary>
    internal class PutNearBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "cacheNear";

        /** Native cache wrapper. */
        private ICache<int, Employee> _cache;

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();

            _cache = Node.GetCache<int, Employee>(CacheName);
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("PutNear", Put, 1));
        }
        
        /// <summary>
        /// Cache put.
        /// </summary>
        private void Put(BenchmarkState state)
        {
            int idx = BenchmarkUtils.GetRandomInt(Dataset);

            _cache.Put(idx, Emps[idx]);
        }
    }
}
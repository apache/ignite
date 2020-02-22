namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache Get benchmark.
    /// </summary>
    internal class GetNearBenchmark : PlatformBenchmarkBase
    {
        /** Cache name. */
        private const string CacheName = "cacheNear";

        /** Native cache wrapper. */
        private ICache<int, Employee> _cache;

        /// <summary>
        /// Gets the cache.
        /// </summary>
        protected ICache<int, Employee> Cache
        {
            get { return _cache; }
        }

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            base.OnStarted();
            
            BatchSize = 1000;

            _cache = Node.GetCache<int, Employee>(CacheName);

            for (int i = 0; i < Emps.Length; i++)
                _cache.Put(i, Emps[i]);
        }

        /** <inheritDoc /> */
        protected override void GetDescriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("GetNear", Get, 1));
        }
        
        /// <summary>
        /// Cache get.
        /// </summary>
        private void Get(BenchmarkState state)
        {
            var idx = BenchmarkUtils.GetRandomInt(Dataset);

            _cache.Get(idx);
        }
    }
}
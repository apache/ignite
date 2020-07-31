namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using System.Threading;
    using Apache.Ignite.BenchmarkDotNet.Models;
    using Apache.Ignite.Core.Client.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Cache put benchmarks.
    /// </summary>
    public class ThinClientCachePutBenchmark : ThinClientBenchmarkBase
    {
        /** */
        private ICacheClient<int, object> _cache;

        /** */
        private static int _counter;
        
        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            _cache = Client.GetOrCreateCache<int, object>("c");
        }

        /// <summary>
        /// Put primitive benchmark.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void PutPrimitive()
        {
            _cache.Put(1, 1);
        }

        /// <summary>
        /// PutAsync benchmark.
        /// </summary>
        [Benchmark]
        public void PutPrimitiveAsync()
        {
            _cache.PutAsync(1, GetNextInt()).Wait();
        }

        /// <summary>
        /// Class with int field benchmark.
        /// </summary>
        [Benchmark]
        public void PutClassWithIntField()
        {
            var obj = new ClassWithIntField
            {
                Foo = GetNextInt()
            };
            
            _cache.PutAsync(1, obj).Wait();
        }

        /// <summary>
        /// Class with int field benchmark.
        /// </summary>
        [Benchmark]
        public void PutClassWithEnumFieldBenchmark()
        {
            var obj = new ClassWithEnumField
            {
                BenchmarkEnum = (BenchmarkEnum)(GetNextInt() % 3)
            };
            
            _cache.PutAsync(1, obj).Wait();
        }

        /// <summary>
        /// Gets the next integer.
        /// </summary>
        private static int GetNextInt()
        {
            return Interlocked.Increment(ref _counter);
        }
    }
}
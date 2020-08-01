namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using System.Threading;
    using Apache.Ignite.BenchmarkDotNet.Models;
    using Apache.Ignite.Core.Client.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Cache put benchmarks.
    /// <para />
    /// |                         Method |      Mean |    Error |   StdDev | Ratio | RatioSD |
    /// |------------------------------- |----------:|---------:|---------:|------:|--------:|
    /// |                   PutPrimitive |  44.54 us | 0.886 us | 1.551 us |  1.00 |    0.00 |
    /// |              PutPrimitiveAsync |  65.24 us | 1.290 us | 1.434 us |  1.45 |    0.05 |
    /// |           PutClassWithIntField |  67.21 us | 1.275 us | 1.518 us |  1.50 |    0.05 |
    /// | PutClassWithEnumFieldBenchmark | 130.13 us | 2.472 us | 2.428 us |  2.88 |    0.08 |
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
        public void PutClassWithEnumField()
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
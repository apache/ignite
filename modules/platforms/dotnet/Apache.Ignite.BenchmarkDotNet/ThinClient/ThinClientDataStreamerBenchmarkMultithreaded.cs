namespace Apache.Ignite.BenchmarkDotNet.ThinClient
{
    using System.Threading.Tasks;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using global::BenchmarkDotNet.Attributes;

    /// <summary>
    /// Thin vs Thick client data streamer benchmark.
    /// <para />
    /// Results on Core i7-9700K, Ubuntu 20.04, .NET Core 5.0.5:
    /// Thin Client: new streamer for every batch.
    /// TODO
    /// |            Method |       Mean |     Error |    StdDev | Ratio | RatioSD |      Gen 0 |      Gen 1 |     Gen 2 | Allocated |
    /// |------------------ |-----------:|----------:|----------:|------:|--------:|-----------:|-----------:|----------:|----------:|
    /// |  StreamThinClient | 3,092.8 ms | 197.78 ms | 583.17 ms |  9.41 |    1.87 | 83000.0000 | 17000.0000 | 3000.0000 | 457.62 MB |
    /// | StreamThickClient |   328.3 ms |   6.54 ms |  11.80 ms |  1.00 |    0.00 | 10000.0000 |  2000.0000 |         - |  63.24 MB |
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientDataStreamerBenchmarkMultithreaded : ThinClientBenchmarkBase
    {
        /** */
        private const string CacheName = "c";

        /** */
        private const int EntryCount = 650000;

        /** */
        public IIgnite ThickClient { get; set; }

        /** */
        public ICache<int,int> Cache { get; set; }

        /** <inheritdoc /> */
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // 3 servers in total.
            Ignition.Start(Utils.GetIgniteConfiguration());
            Ignition.Start(Utils.GetIgniteConfiguration());

            ThickClient = Ignition.Start(Utils.GetIgniteConfiguration(client: true));

            Cache = ThickClient.CreateCache<int, int>(CacheName);
        }

        [IterationSetup]
        public void Setup()
        {
            Cache.Clear();
        }

        /// <summary>
        /// Benchmark: thin client streamer.
        /// </summary>
        [Benchmark]
        public void StreamThinClient()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(CacheName))
            {
                // ReSharper disable once AccessToDisposedClosure
                Parallel.For(0, EntryCount, i => streamer.Add(i, -i));
            }
        }

        /// <summary>
        /// Benchmark: thick client streamer.
        /// </summary>
        [Benchmark(Baseline = true)]
        public void StreamThickClient()
        {
            using (var streamer = ThickClient.GetDataStreamer<int, int>(CacheName))
            {
                // ReSharper disable once AccessToDisposedClosure
                Parallel.For(0, EntryCount, i => streamer.Add(i, -i));
            }
        }
    }
}

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
    /// Volatile buffers:
    /// |            Method |       Mean |     Error |    StdDev | Ratio | RatioSD |      Gen 0 |      Gen 1 |     Gen 2 | Allocated |
    /// |------------------ |-----------:|----------:|----------:|------:|--------:|-----------:|-----------:|----------:|----------:|
    /// |  StreamThinClient | 3,092.8 ms | 197.78 ms | 583.17 ms |  9.41 |    1.87 | 83000.0000 | 17000.0000 | 3000.0000 | 457.62 MB |
    /// | StreamThickClient |   328.3 ms |   6.54 ms |  11.80 ms |  1.00 |    0.00 | 10000.0000 |  2000.0000 |         - |  63.24 MB |
    /// CAS buffers:
    /// |            Method |      Mean |    Error |   StdDev | Ratio | RatioSD |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |------------------ |----------:|---------:|---------:|------:|--------:|----------:|----------:|------:|----------:|
    /// |  StreamThinClient | 106.24 ms | 3.138 ms | 9.004 ms |  1.42 |    0.16 | 4000.0000 | 1000.0000 |     - |  23.51 MB |
    /// | StreamThickClient |  75.23 ms | 1.893 ms | 5.432 ms |  1.00 |    0.00 | 2000.0000 | 1000.0000 |     - |  13.96 MB |
    /// Array buffers:
    /// |           Method |     Mean |   Error |  StdDev |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |----------------- |---------:|--------:|--------:|----------:|----------:|------:|----------:|
    /// | StreamThinClient | 108.4 ms | 2.71 ms | 7.82 ms | 3000.0000 | 1000.0000 |     - |  19.79 MB |
    /// Clear parent links, reduce task alloc:
    /// | StreamThinClient | 106.6 ms | 2.89 ms | 8.43 ms | 2000.0000 |     - |     - |  16.67 MB |
    /// Keep open streamers:
    /// | StreamThinClient | 161.6 ms | 6.09 ms | 17.39 ms | 3000.0000 | 1000.0000 |     - |  18.05 MB |
    /// Keep open streamers, background init:
    /// | StreamThinClient | 144.6 ms | 5.73 ms | 16.88 ms | 2000.0000 |     - |     - |  16.69 MB |
    /// Keep open streamers, do not flush on server when not necessary:
    /// |            Method |     Mean |    Error |   StdDev | Ratio | RatioSD |     Gen 0 |     Gen 1 | Gen 2 | Allocated |
    /// |------------------ |---------:|---------:|---------:|------:|--------:|----------:|----------:|------:|----------:|
    /// |  StreamThinClient | 74.02 ms | 2.036 ms | 5.776 ms |  1.02 |    0.10 | 2000.0000 |         - |     - |  16.58 MB |
    /// | StreamThickClient | 73.05 ms | 1.458 ms | 3.409 ms |  1.00 |    0.00 | 2000.0000 | 1000.0000 |     - |  13.83 MB |
    /// </summary>
    [MemoryDiagnoser]
    public class ThinClientDataStreamerBenchmarkMultithreaded : ThinClientBenchmarkBase
    {
        /** */
        private const string CacheName = "c";

        /** */
        private const int EntryCount = 150000;

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

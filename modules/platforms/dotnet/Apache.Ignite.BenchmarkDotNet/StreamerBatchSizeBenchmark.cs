namespace Apache.Ignite.BenchmarkDotNet
{
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using global::BenchmarkDotNet.Attributes;

    public class StreamerBatchSizeBenchmark
    {
        public IIgnite Ignite { get; set; }

        public ICache<int, int> Cache { get; set; }

        /// <summary>
        /// Sets up the benchmark.
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            Ignite = Ignition.Start(Utils.GetIgniteConfiguration());
            Cache = Ignite.GetOrCreateCache<int, int>("c");
        }

        /// <summary>
        /// Cleans up the benchmark.
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            Ignite.Dispose();
        }

        [Benchmark]
        public void OneStreamerManyBatches()
        {
            using (var streamer = Ignite.GetDataStreamer<int, int>(Cache.Name))
            {
                for (int i = 0; i < 50000; i++)
                {
                    streamer.AddData(i, i);
                }
            }
        }

        [Benchmark]
        public void OneBatchManyStreamers()
        {
            for (int i = 0; i < 50; i++)
            {
                using (var streamer = Ignite.GetDataStreamer<int, int>(Cache.Name))
                {
                    for (int j = 0; j < 1000; j++)
                    {
                        streamer.AddData(-i * j, -i * j);
                    }
                }
            }
        }
    }
}

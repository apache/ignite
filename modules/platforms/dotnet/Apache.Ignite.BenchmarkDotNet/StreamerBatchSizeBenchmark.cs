namespace Apache.Ignite.BenchmarkDotNet
{
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using global::BenchmarkDotNet.Attributes;

    public class StreamerBatchSizeBenchmark
    {
        private const int BaseCount = 1024;
        private const int Count = BaseCount*100;
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
                for (int i = 0; i < Count; i++)
                {
                    streamer.AddData(i, i);
                }
            }
        }

        [Benchmark]
        public void OneBatchManyStreamers()
        {
            const int batchSize = BaseCount * 10;

            for (int i = 0; i < (Count / batchSize); i++)
            {
                using (var streamer = Ignite.GetDataStreamer<int, int>(Cache.Name))
                {
                    var offs = i * batchSize;

                    for (int j = 0; j < batchSize; j++)
                    {
                        streamer.AddData(offs + j, offs + j);
                    }
                }
            }
        }
    }
}

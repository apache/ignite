package org.apache.ignite.internal.benchmarks.jmh.streamer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xms1g", "-Xmx1g", "-server", "-XX:+AggressiveOpts", "-XX:MaxMetaspaceSize=256m"})
@State(Scope.Benchmark)
@Threads(1)
@Measurement(iterations = 7, batchSize = 1)
@Warmup(iterations = 3, batchSize = 1)
public class JmhStreamerReceiverBenchmark {
    /** */
    private static final int SERVERS = 3;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final long REGION_SIZE = 512 * 1024L * 1024L;

    /** */
    private static final int ENTRIES_TO_LOAD = 200_000;

    /** */
    private static final int VALUES_BANK_SIZE = 2000;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private Object[] values;

    /** */
    private Ignite loaderNode;

    /** */
    private List<Ignite> nodes;

    /**
     * Create Ignite configuration.
     *
     * @return Ignite configuration.
     */
    private IgniteConfiguration getConfiguration(String instName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(instName);

        cfg.setWorkDirectory(workDirectory(instName));

        if (isClient)
            cfg.setClientMode(true);
        else {
            cfg.setGridLogger(new NullLogger());

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setPageSize(DataStorageConfiguration.DFLT_PAGE_SIZE)
                .setWalMode(WALMode.NONE)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(REGION_SIZE))
                .setCheckpointFrequency(3000));

            CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME);
            ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            ccfg.setBackups(BACKUPS);
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
            ccfg.setCacheMode(CacheMode.REPLICATED);
            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** */
    private String workDirectory(String instName) {
        return System.getProperty("java.io.tmpdir") + File.separator + "ignite" + File.separator + instName;
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
//        System.err.println("tearDown()");
//        loaderNode.close();

        nodes.forEach(n -> {
            String workDir = n.configuration().getWorkDirectory();

            try {
                n.close();
            }
            catch (Exception e) {
                // No-op
            }

            IgniteUtils.delete(new File(workDir));
        });
    }

    /**
     * Start 2 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        System.err.println("TEST | setup(), starting grid...");

        nodes = new ArrayList<>(SERVERS + 1);

        for (int i = 0; i < SERVERS; ++i) {
            IgniteConfiguration cfg = getConfiguration("srv" + i, false);

//            System.out.println("Work dir: " + cfg.getWorkDirectory());

            IgniteUtils.delete(new File(cfg.getWorkDirectory()));

            nodes.add(Ignition.start(cfg));

//            System.out.println("Starting server " + i);
        }

        nodes.get(0).cluster().state(ClusterState.ACTIVE);

        nodes.add(loaderNode = Ignition.start(getConfiguration("client", true)));
//        loaderNode = servers[2];

        values = new Object[VALUES_BANK_SIZE];
    }

    @Setup(Level.Iteration)
    public void randomizeLoad() {
        System.out.println("Randomizing load...");

        nodes.get(0).cache(CACHE_NAME).clear();

        assert nodes.get(0).cache(CACHE_NAME).size() == 0;

        for (int i = 0; i < values.length; i++) {
            int valLen = ThreadLocalRandom.current().nextInt(4, 7 * 1024);

            StringBuilder sb = new StringBuilder();

            for (int j = 0; j < valLen; j++)
                sb.append('a' + ThreadLocalRandom.current().nextInt(20));

            values[i] = sb.toString();
        }
    }

    /**
     * Test with default receiver.
     */
    @Benchmark
    public void defaultIsolatedReceiver() {
        doTest(null);
    }

    /**
     * Test with batcher receiver.
     */
//    @Benchmark
    public void batchedReceiver() {
        doTest(DataStreamerCacheUpdaters.batched());
    }

    /**
     * Test with batcher receiver.
     */
//    @Benchmark
    public void batchedSortedReceiver() {
        doTest(DataStreamerCacheUpdaters.batchedSorted());
    }

    /**
     * Test with batcher receiver.
     */
//    @Benchmark
    public void individualReceiver() {
        doTest(DataStreamerCacheUpdaters.individual());
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
            .include(JmhStreamerReceiverBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }

    /** */
    private void doTest(@Nullable StreamReceiver<Integer, Object> receiver) {
        System.err.println("TEST | doTest");

        try (IgniteDataStreamer<Integer, Object> streamer = loaderNode.dataStreamer(CACHE_NAME)) {
            if (receiver != null)
                streamer.receiver(receiver);

            for (int e = 0; e < ENTRIES_TO_LOAD; ++e) {
//                System.out.println("Added entry " + e);

                streamer.addData(e, values[e % values.length]);
            }
        }

        nodes.stream().filter(n -> !n.configuration().isClientMode()).map(n -> (IgniteEx)n).forEach(n -> {
            try {
                n.context().cache().context().database().waitForCheckpoint("forced");
            }
            catch (IgniteCheckedException e) {
                n.log().error("Unable to wait for checkpoint.", e);
            }
        });

        assert nodes.get(0).cache(CACHE_NAME).size() == ENTRIES_TO_LOAD;
    }
}

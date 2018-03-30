package org.apache.ignite.yardstick.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.jetbrains.annotations.NotNull;
import org.yardstickframework.BenchmarkUtils;

public class Loader implements IgniteClosure<Integer, Integer> {
//    private static AtomicBoolean invoked = new AtomicBoolean();

    private AtomicBoolean loaded = new AtomicBoolean();


    private IgniteCache<Integer, SampleValue> cache;

    private IgniteBenchmarkArguments args;

    private Ignite ignite;

    public Loader(
        IgniteCache<Integer, SampleValue> cache, IgniteBenchmarkArguments args, Ignite ignite) {
        this.cache = cache;
        this.args = args;
        this.ignite = ignite;
    }

    @Override public Integer apply(Integer integer) {
//        IgniteCache<Integer, SampleValue> cache = (IgniteCache<Integer, SampleValue>)cacheForOperation();

//        if(check(cache)) {
//            BenchmarkUtils.println("Check method returned true");
//
//            return null;
//        }
//        else
//            BenchmarkUtils.println("Check method returned false");

//        if(invoked()){
//            BenchmarkUtils.println("Preload has already been invoked");
//
//            while(!loaded.get()){
//                BenchmarkUtils.println("Waiting for preload to complete.");
//
//                try {
//                    Thread.sleep(1000L);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            return null;
//        }
//        else{
//            BenchmarkUtils.println("Preload has not been invoked");
//        }


        CacheConfiguration<Integer, SampleValue> cc = cache.getConfiguration(CacheConfiguration.class);

        String dataRegName = cc.getDataRegionName();

        BenchmarkUtils.println("Data region name = " + dataRegName);

        DataStorageConfiguration dataStorCfg = ignite.configuration().getDataStorageConfiguration();

        int pageSize = dataStorCfg.getPageSize();

        BenchmarkUtils.println("Page size = " + pageSize);

        DataRegionConfiguration dataRegCfg = null;

        DataRegionConfiguration[] arr = ignite.configuration().getDataStorageConfiguration()
            .getDataRegionConfigurations();

        for (DataRegionConfiguration cfg : arr) {
            if (cfg.getName().equals(dataRegName))
                dataRegCfg = cfg;
        }

        if (dataRegCfg == null) {
            BenchmarkUtils.println(String.format("Failed to get data region configuration for cache %s",
                cache.getName()));

            return null;
        }

        long maxSize = dataRegCfg.getMaxSize();

        BenchmarkUtils.println("Max size = " + maxSize);

        long initSize = dataRegCfg.getInitialSize();

        if (maxSize != initSize)
            BenchmarkUtils.println("Initial data region size must be equal to max size!");

        long pageNum = maxSize / pageSize;

        BenchmarkUtils.println("Pages in data region: " + pageNum);

        int cnt = 0;

        final long pagesToLoad = pageNum * args.preloadDataRegionMult();

        IgniteEx igniteEx = (IgniteEx)ignite;

        try {
            final DataRegionMetricsImpl impl = igniteEx.context().cache().context().database().dataRegion(dataRegName)
                .memoryMetrics();

            impl.enableMetrics();

            BenchmarkUtils.println("Initial allocated pages = " + impl.getTotalAllocatedPages());

            ExecutorService serv = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r, "Preload checker");
                }
            });

            Future<?> fut = serv.submit(new Runnable() {
                @Override public void run()  {
                    while (!loaded.get()) {

                        if (impl.getTotalAllocatedPages() >= pagesToLoad)
                            loaded.getAndSet(true);

                        try {
                            Thread.sleep(500L);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });

            try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cache.getName())) {
                while (!loaded.get()) {
                    streamer.addData(cnt++, new SampleValue());

                    if (cnt % 1000_000 == 0) {
                        long allocPages = impl.getTotalAllocatedPages();

                        BenchmarkUtils.println("Load count = " + cnt);

                        BenchmarkUtils.println("Allocated pages = " + allocPages);
                    }
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }

            try {
                fut.get();
            }
            catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            serv.shutdown();

            impl.disableMetrics();

            BenchmarkUtils.println("Objects loaded = " + cnt);
            BenchmarkUtils.println("Total allocated pages = " + impl.getTotalAllocatedPages());

        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }


        return cnt;
    }

    private synchronized boolean check(IgniteCache<Integer, SampleValue> cache){
        if(cache.get(Integer.MAX_VALUE) == null){
            BenchmarkUtils.println("Key MAX_VALUE is not found");

            while(cache.get(Integer.MAX_VALUE) == null) {
                cache.put(Integer.MAX_VALUE, new SampleValue());

                try {
                    Thread.sleep(1000L);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return false;
        }
        else {
            BenchmarkUtils.println("Key MAX_VALUE is found");

            return true;
        }
    }

//    private synchronized boolean invoked(){
//        if(!invoked.get()){
//            invoked.getAndSet(true);
//
//            return false;
//        }
//        return true;
//    }
}
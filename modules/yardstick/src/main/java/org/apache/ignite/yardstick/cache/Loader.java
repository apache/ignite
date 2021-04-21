/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * Class for preload data before benchmarking PDS.
 */
public class Loader implements IgniteClosure<Integer, Integer> {
    /** */
    private AtomicBoolean loaded = new AtomicBoolean();

    /** */
    private IgniteCache<Integer, SampleValue> cache;

    /** */
    private IgniteBenchmarkArguments args;

    /** */
    private Ignite ignite;

    /**
     * Constructor.
     *
     * @param cache cache to preload data.
     * @param args arguments.
     * @param ignite Ignite instance.
     */
    Loader(IgniteCache<Integer, SampleValue> cache, IgniteBenchmarkArguments args, Ignite ignite) {
        this.cache = cache;
        this.args = args;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public Integer apply(Integer integer) {
        CacheConfiguration<Integer, SampleValue> cc = cache.getConfiguration(CacheConfiguration.class);

        String dataRegName = cc.getDataRegionName() == null ?
            ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName() :
            cc.getDataRegionName();

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
                @Override public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r, "Preload checker");
                }
            });

            Future<?> checkFut = serv.submit(new Runnable() {
                @Override public void run() {
                    while (!loaded.get()) {
                        if (impl.getTotalAllocatedPages() >= pagesToLoad)
                            loaded.getAndSet(true);

                        try {
                            Thread.sleep(500L);
                        }
                        catch (InterruptedException e) {
                            BenchmarkUtils.error("Was interrupted while waiting before next check.", e);
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
            catch (Exception e) {
                BenchmarkUtils.error("Failed to load data.", e);
            }

            try {
                checkFut.get();
            }
            catch (InterruptedException | ExecutionException e) {
                BenchmarkUtils.error("Failed to check loading.", e);
            }
            finally {
                serv.shutdown();
            }

            impl.disableMetrics();

            BenchmarkUtils.println("Objects loaded = " + cnt);

            BenchmarkUtils.println("Total allocated pages = " + impl.getTotalAllocatedPages());
        }
        catch (IgniteCheckedException e) {
            BenchmarkUtils.error("Failed to load data.", e);
        }

        return cnt;
    }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbe;
import org.yardstickframework.BenchmarkProbePoint;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.yardstickframework.BenchmarkUtils.errorHelp;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Probe which calculate continuous query events.
 */
public class CacheEntryEventProbe implements BenchmarkProbe {
    /** */
    private BenchmarkConfiguration cfg;

    /** Collected points. */
    private Collection<BenchmarkProbePoint> collected = new ArrayList<>();

    /** Query cursor. */
    private QueryCursor qryCur;

    /** Service building probe points. */
    private ExecutorService buildingService;

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) throws Exception {
        this.cfg = cfg;

        if (drv instanceof IgniteCacheAbstractBenchmark) {
            IgniteCacheAbstractBenchmark drv0 = (IgniteCacheAbstractBenchmark)drv;

            if (drv0.cache() != null) {
                ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

                final AtomicLong cnt = new AtomicLong();

                qry.setLocalListener(localListener(cnt));

                qryCur = drv0.cache().query(qry);

                buildingService = Executors.newSingleThreadExecutor();

                buildingService.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            while (!Thread.currentThread().isInterrupted()) {
                                Thread.sleep(1000);

                                long evts = cnt.getAndSet(0);

                                BenchmarkProbePoint pnt = new BenchmarkProbePoint(
                                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                    new double[] {evts});

                                collectPoint(pnt);
                            }
                        }
                        catch (InterruptedException e) {
                            // No-op.
                        }
                    }
                });

                println(cfg, getClass().getSimpleName() + " probe is started.");
            }
        }

        if (qryCur == null)
            errorHelp(cfg, "Can not start " + getClass().getSimpleName()
                + " probe. Probably, the driver doesn't provide \"cache()\" method.");
    }

    /**
     * @param cntr Received event counter.
     * @return Local listener.
     */
    protected CacheEntryUpdatedListener<Integer, Integer> localListener(final AtomicLong cntr) {
        return new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> events)
                throws CacheEntryListenerException {
                int size = 0;

                for (CacheEntryEvent<? extends Integer, ? extends Integer> e : events)
                    ++size;

                cntr.addAndGet(size);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        if (qryCur != null) {
            qryCur.close();

            qryCur = null;

            buildingService.shutdownNow();

            buildingService.awaitTermination(1, MINUTES);

            println(cfg, getClass().getSimpleName() + " is stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList("Time, sec", "Received events/sec (more is better)");
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<BenchmarkProbePoint> points() {
        Collection<BenchmarkProbePoint> ret = collected;

        collected = new ArrayList<>(ret.size() + 5);

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void buildPoint(long time) {
        // No-op.
    }

    /**
     * @param pnt Probe point.
     */
    private synchronized void collectPoint(BenchmarkProbePoint pnt) {
        collected.add(pnt);
    }
}

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

package org.apache.ignite.yardstick.probes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbePoint;
import org.yardstickframework.BenchmarkTotalsOnlyProbe;

import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Pages statistics probe.
 */
public class PagesStatProbe implements BenchmarkTotalsOnlyProbe {
    /** */
    private volatile LongMetric readPages;

    /** */
    private volatile LongMetric readPagesTime;

    /** */
    private volatile LongMetric replPages;

    /** */
    private volatile LongMetric replPagesTime;

    /** */
    private volatile long prevReadPages;

    /** */
    private volatile long prevReadPagesTime;

    /** */
    private volatile long prevReplPages;

    /** */
    private volatile long prevReplPagesTime;

    /** Collected points. */
    private Collection<BenchmarkProbePoint> collected = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) throws Exception {
        if (drv instanceof IgniteAbstractBenchmark) {
            IgniteEx ignite = (IgniteEx)((IgniteAbstractBenchmark)drv).ignite();

            ReadOnlyMetricRegistry mreg = ignite.context().metric().registry(metricName(DATAREGION_METRICS_PREFIX,
                ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName()));

            readPages = mreg.findMetric("PagesRead");
            readPagesTime = mreg.findMetric("PagesReadTime");
            replPages = mreg.findMetric("PagesReplaced");
            replPagesTime = mreg.findMetric("PagesReplaceTime");
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList("Time, sec",
            "Total read pages time, nanos", "Read pages count", "Avg read page time, nanos",
            "Total page replacement time, nanos", "Replaced pages count", "Avg replace page time, nanos");
    }

    /** {@inheritDoc} */
    @Override public Collection<BenchmarkProbePoint> points() {
        Collection<BenchmarkProbePoint> ret = collected;

        collected = new ArrayList<>(ret.size());

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void buildPoint(long time) {
        long curReadPages = readPages.value();
        long curReadPagesTime = readPagesTime.value();
        long curReplPages = replPages.value();
        long curReplPagesTime = replPagesTime.value();

        collected.add(new BenchmarkProbePoint(TimeUnit.MILLISECONDS.toSeconds(time),
            new double[] {
                curReadPagesTime - prevReadPagesTime,
                curReadPages - prevReadPages,
                curReadPages - prevReadPages == 0 ? 0 :
                    1d * (curReadPagesTime - prevReadPagesTime) / (curReadPages - prevReadPages),
                curReplPagesTime - prevReplPagesTime,
                curReplPages - prevReplPages,
                curReplPages - prevReplPages == 0 ? 0 :
                    1d * (curReplPagesTime - prevReplPagesTime) / (curReplPages - prevReplPages)
        }));

        prevReadPages = curReadPages;
        prevReadPagesTime = curReadPagesTime;
        prevReplPages = curReplPages;
        prevReplPagesTime = curReplPagesTime;
    }
}

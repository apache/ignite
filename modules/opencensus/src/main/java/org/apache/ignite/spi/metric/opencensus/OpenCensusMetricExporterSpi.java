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

package org.apache.ignite.spi.metric.opencensus;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import io.opencensus.common.Scope;
import io.opencensus.stats.Aggregation.LastValue;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagMetadata;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tags;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.jetbrains.annotations.Nullable;

import static io.opencensus.tags.TagMetadata.TagTtl.UNLIMITED_PROPAGATION;

/**
 * <a href="https://opencensus.io">OpenCensus</a> monitoring exporter. <br>
 * <br>
 * This class will export all Ignite metrics with the OpenCensus API.<br>
 * <br>
 * Please, note, metrics recorded with the OpenCensus API each {@link #period} milliseconds.
 * <br>
 * To enable export from OpenCensus to the wild user should configure OpenCensus exporter.
 * Please, see <a href="https://opencensus.io/exporters/supported-exporters/java/">OpenCensus documentation</a> for additional information.
 *
 * Example of exporter configuration:
 * <pre>
 * {@code
 *   PrometheusStatsCollector.createAndRegister();
 *
 *   HTTPServer server = new HTTPServer("localhost", 8888, true);
 * }
 * </pre>
 *
 * @see MetricRegistry
 * @see GridMetricManager
 * @see ReadOnlyMetricManager
 */
public class OpenCensusMetricExporterSpi extends PushMetricsExporterAdapter {
    /** Flag to enable or disable tag with Ignite instance name. */
    private boolean sendInstanceName;

    /** Flag to enable or disable tag with Node id. */
    private boolean sendNodeId;

    /** Flag to enable or disable tag with Consistent id. */
    private boolean sendConsistentId;

    /** Ignite instance name. */
    public static final TagKey INSTANCE_NAME_TAG = TagKey.create("iin");

    /** Ignite node id. */
    public static final TagKey NODE_ID_TAG = TagKey.create("ini");

    /** Ignite node consistent id. */
    public static final TagKey CONSISTENT_ID_TAG = TagKey.create("inci");

    /** Tags metadata. */
    public static final TagMetadata METADATA = TagMetadata.create(UNLIMITED_PROPAGATION);

    /** Ignite instance name in the form of {@link TagValue}. */
    private TagValue instanceNameValue;

    /** Ignite node id in the form of {@link TagValue}. */
    private TagValue nodeIdValue;

    /** Ignite consistent id in the form of {@link TagValue}. */
    private TagValue consistenIdValue;

    /**
     * Tags that will be exported with each measure.
     *
     * @see #sendInstanceName
     * @see #sendNodeId
     */
    private List<TagKey> tags = new ArrayList<>();

    /**
     * Opencensus measures.
     * Values obtained from Ignite recorded to them.
     */
    private Map<String, Measure> measures = new HashMap<>();

    /** Cached histogram metrics intervals names. */
    private final Map<String, T2<long[], String[]>> histogramNames = new HashMap<>();

    /** */
    private static final Function<Metric, Measure> CREATE_LONG = m ->
        MeasureLong.create(m.name(), m.description() == null ? m.name() : m.description(), "");

    /** */
    private static final Function<Metric, Measure> CREATE_DOUBLE = m ->
        MeasureDouble.create(m.name(), m.description() == null ? m.name() : m.description(), "");

    /** {@inheritDoc} */
    @Override public void export() {
        StatsRecorder recorder = Stats.getStatsRecorder();

        try (Scope globalScope = tagScope()) {
            MeasureMap mmap = recorder.newMeasureMap();

            mreg.forEach(mreg -> {
                if (filter != null && !filter.test(mreg))
                    return;

                mreg.forEach(metric -> {
                    if (metric instanceof LongMetric ||
                        metric instanceof IntMetric ||
                        metric instanceof BooleanMetric ||
                        (metric instanceof ObjectMetric && ((ObjectMetric)metric).type() == Date.class) ||
                        (metric instanceof ObjectMetric && ((ObjectMetric)metric).type() == OffsetDateTime.class)) {
                        long val;

                        if (metric instanceof LongMetric)
                            val = ((LongMetric)metric).value();
                        else if (metric instanceof IntMetric)
                            val = ((IntMetric)metric).value();
                        else if (metric instanceof BooleanMetric)
                            val = ((BooleanMetric)metric).value() ? 1 : 0;
                        else if (metric instanceof ObjectMetric && ((ObjectMetric)metric).type() == Date.class)
                            val = ((ObjectMetric<Date>)metric).value().getTime();
                        else
                            val = ((ObjectMetric<OffsetDateTime>)metric).value().toInstant().toEpochMilli();

                        if (val < 0) {
                            if (log.isDebugEnabled())
                                log.debug("OpenCensus doesn't support negative values. Skip record of " + metric.name());

                            return;
                        }

                        MeasureLong msr = (MeasureLong)measures.computeIfAbsent(metric.name(),
                            k -> createMeasure(metric, CREATE_LONG));

                        mmap.put(msr, val);
                    }
                    else if (metric instanceof DoubleMetric) {
                        double val = ((DoubleMetric)metric).value();

                        if (val < 0) {
                            if (log.isDebugEnabled())
                                log.debug("OpenCensus doesn't support negative values. Skip record of " + metric.name());

                            return;
                        }

                        MeasureDouble msr = (MeasureDouble)measures.computeIfAbsent(metric.name(),
                            k -> createMeasure(metric, CREATE_DOUBLE));

                        mmap.put(msr, val);
                    }
                    else if (metric instanceof HistogramMetric) {
                        String[] names = histogramBucketNames((HistogramMetric)metric);
                        long[] vals = ((HistogramMetric)metric).value();

                        assert names.length == vals.length;

                        for (int i = 0; i < vals.length; i++) {
                            String name = names[i];

                            MeasureLong msr = (MeasureLong)measures.computeIfAbsent(name,
                                k -> createMeasureLong(name, metric.description()));

                            mmap.put(msr, vals[i]);
                        }
                    }
                    else if (log.isDebugEnabled()) {
                        log.debug(metric.name() +
                            "[" + metric.getClass() + "] not supported by Opencensus exporter");
                    }
                });
            });

            mmap.record();
        }
    }

    /** */
    private Scope tagScope() {
        TagContextBuilder builder = Tags.getTagger().currentBuilder();

        if (sendInstanceName)
            builder.put(INSTANCE_NAME_TAG, instanceNameValue, METADATA);

        if (sendNodeId)
            builder.put(NODE_ID_TAG, nodeIdValue, METADATA);

        if (sendConsistentId)
            builder.put(CONSISTENT_ID_TAG, consistenIdValue, METADATA);

        return builder.buildScoped();
    }

    /** */
    private Measure createMeasure(Metric m, Function<Metric, Measure> factory) {
        Measure msr = factory.apply(m);

        addView(msr);

        return msr;
    }

    /** */
    private MeasureLong createMeasureLong(String name, String desc) {
        MeasureLong msr = MeasureLong.create(name, desc == null ? name : desc, "");

        addView(msr);

        return msr;
    }

    /** */
    private void addView(Measure msr) {
        View v = View.create(Name.create(msr.getName()), msr.getDescription(), msr, LastValue.create(), tags);

        Stats.getViewManager().registerView(v);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        super.spiStart(igniteInstanceName);

        if (sendInstanceName) {
            tags.add(INSTANCE_NAME_TAG);

            instanceNameValue = TagValue.create(igniteInstanceName);
        }

        if (sendNodeId) {
            tags.add(NODE_ID_TAG);

            nodeIdValue = TagValue.create(((IgniteEx)ignite()).context().localNodeId().toString());
        }

        if (sendConsistentId) {
            tags.add(CONSISTENT_ID_TAG);

            //Node consistent id will be known in #onContextInitialized0(IgniteSpiContext), after DiscoMgr started.
            consistenIdValue = TagValue.create("unknown");
        }

        mreg.addMetricRegistryRemoveListener(mreg -> mreg.forEach(metric -> histogramNames.remove(metric.name())));
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);

        consistenIdValue = TagValue.create(
            ((IgniteEx)ignite()).context().discovery().localNode().consistentId().toString());
    }

    /**
     * @param metric Histogram metric.
     * @return Histogram intervals names.
     */
    private String[] histogramBucketNames(HistogramMetric metric) {
        String name = metric.name();
        long[] bounds = metric.bounds();

        T2<long[], String[]> tuple = histogramNames.get(name);

        if (tuple != null && tuple.get1() == bounds)
            return tuple.get2();

        String[] names = MetricUtils.histogramBucketNames(metric);

        histogramNames.put(name, new T2<>(bounds, names));

        return names;
    }

    /**
     * If {@code true} then {@link #INSTANCE_NAME_TAG} will be added to each exported measure.
     *
     * @param sendInstanceName Flag value.
     */
    public void setSendInstanceName(boolean sendInstanceName) {
        this.sendInstanceName = sendInstanceName;
    }

    /**
     * If {@code true} then {@link #NODE_ID_TAG} will be added to each exported measure.
     *
     * @param sendNodeId Flag value.
     */
    public void setSendNodeId(boolean sendNodeId) {
        this.sendNodeId = sendNodeId;
    }

    /**
     * If {@code true} then {@link #CONSISTENT_ID_TAG} will be added to each exported measure.
     *
     * @param sendConsistentId Flag value.
     */
    public void setSendConsistentId(boolean sendConsistentId) {
        this.sendConsistentId = sendConsistentId;
    }
}

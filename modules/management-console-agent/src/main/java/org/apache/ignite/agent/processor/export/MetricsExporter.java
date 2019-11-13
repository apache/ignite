/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor.export;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.dto.metric.MetricRegistrySchema;
import org.apache.ignite.agent.dto.metric.MetricRequest;
import org.apache.ignite.agent.dto.metric.MetricResponse;
import org.apache.ignite.agent.dto.metric.MetricSchema;
import org.apache.ignite.agent.dto.metric.MetricType;
import org.apache.ignite.agent.dto.metric.VarIntWriter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;

import static org.apache.ignite.agent.dto.metric.MetricType.BOOLEAN;
import static org.apache.ignite.agent.dto.metric.MetricType.DOUBLE;
import static org.apache.ignite.agent.dto.metric.MetricType.HISTOGRAM;
import static org.apache.ignite.agent.dto.metric.MetricType.HIT_RATE;
import static org.apache.ignite.agent.dto.metric.MetricType.INT;
import static org.apache.ignite.agent.dto.metric.MetricType.LONG;
import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;

/**
 * Ignite processor which writes the metrics in a compact binary format for Management Console:
 *
 * Header
 * Schema
 * Reg schemas idx
 * Reg schemas
 * Data
 */
public class MetricsExporter extends GridProcessorAdapter {
    /** Default varInt byte buffer capacity. */
    private static final int DEFAULT_VARINT_BYTE_BUF_CAPACITY = 2048;

    /** Listener. */
    private final GridMessageListener lsnr;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public MetricsExporter(GridKernalContext ctx) {
        super(ctx);

        this.lsnr = (UUID nodeId, Object msg, byte plc) -> {
            if (msg instanceof MetricRequest) {
                MetricResponse res = export();

                try {
                    ctx.io().sendToGridTopic(nodeId, TOPIC_METRICS, res, plc);
                }
                catch (IgniteCheckedException e) {
                    log.error("Error during sending message [topic=" + TOPIC_METRICS +
                        ", dstNodeId=" + nodeId + ", msg=" + msg + ']');
                }
            }
        };
    }

    /**
     * Add metric listener.
     */
    public void addMetricListener() {
        ctx.io().addMessageListener(TOPIC_METRICS, lsnr);
    }

    /**
     * Creates {@link MetricResponse} message instance.
     *
     * @return Metric response.
     */
    public MetricResponse export() {
        IgniteCluster cluster = ctx.cluster().get();

        UUID clusterId = cluster.id();

        assert clusterId != null : "ClusterId is null.";

        String tag = cluster.tag();

        String consistentId = ctx.discovery().localNode().consistentId().toString();

        assert consistentId != null : "ConsistentId is null.";

        Map<String, MetricRegistry> metrics = deepCopyRegistries(ctx.metric().registries());

        return metricMessage(clusterId, tag, consistentId, metrics);
    }


    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.io().removeMessageListener(TOPIC_METRICS, lsnr);
    }

    /**
     * Creates {@link MetricResponse} message for given parameters and metric registries.
     *
     * @param clusterId Cluster ID.
     * @param clusterTag Cluster tag.
     * @param consistentId Consistent node ID.
     * @param metrics Metric registries.
     * @return {@link MetricResponse} instance.
     */
    MetricResponse metricMessage(
            UUID clusterId,
            String clusterTag,
            String consistentId,
            Map<String, MetricRegistry> metrics
    ) {
        long ts = System.currentTimeMillis();

        MetricSchema schema = generateSchema(metrics);

        byte[] schemaBytes = schema.toBytes();

        VarIntWriter data = generateData(metrics);

        return new MetricResponse(
                -1, //Unsupported.
                ts,
                clusterId,
                clusterTag,
                consistentId,
                schema.length(),
                data.position(),
                (arr, off) -> writeSchema(schemaBytes, arr, off),
                (arr, off) -> writeData(data, arr, off)
        );
    }

    /**
     * Generates metrics schema for given metric registries.
     *
     * @param metrics Metric registries.
     * @return Schema for metric message.
     */
    private MetricSchema generateSchema(Map<String, MetricRegistry> metrics) {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (MetricRegistry reg : metrics.values()) {
            MetricRegistrySchema regSchema = generateMetricRegistrySchema(reg);

            bldr.add(reg.type(), reg.name(), regSchema);
        }

        return bldr.build();
    }

    /**
     * Generates metric registry schema for given metric registry.
     *
     * @param reg Metric registry.
     * @return Metric registry schema.
     */
    private MetricRegistrySchema generateMetricRegistrySchema(MetricRegistry reg) {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (Map.Entry<String, Metric> e : reg.metrics().entrySet()) {
            String name = e.getKey();

            Metric m = e.getValue();

            MetricType metricType = MetricType.findByClass(m.getClass());

            if (metricType != null)
                bldr.add(name, metricType);
        }

        return bldr.build();
    }

    /**
     * @param metrics Metrics map to copy.
     * @return Deep copy of the map.
     */
    private Map<String, MetricRegistry> deepCopyRegistries(Map<String, MetricRegistry> metrics) {
        Map<String, MetricRegistry> res = U.newHashMap(metrics.size());

        for (Map.Entry<String, MetricRegistry> entry : metrics.entrySet())
            res.put(entry.getKey(), deepCopyMetrics(entry.getValue()));

        return res;
    }

    /**
     * @param registry Registry to copy.
     * @return Registry copy.
     */
    private MetricRegistry deepCopyMetrics(MetricRegistry registry) {
        HashMap<String, Metric> cpy = U.newHashMap(registry.metrics().size());

        for (Map.Entry<String, Metric> entry : registry.metrics().entrySet())
            cpy.put(entry.getKey(), entry.getValue());

        return new MetricRegistry(registry.type(), registry.name(), log, cpy);
    }

    /**
     * Copies metric schema bytes representation to target byte array.
     *
     * @param schemaBytes Metric schema byte representation.
     * @param arr Target byte array.
     * @param off Target byte array offset.
     */
    private static void writeSchema(byte[] schemaBytes, byte[] arr, Integer off) {
        copyMemory(schemaBytes, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, schemaBytes.length);
    }

    /**
     * Copies metric values to target byte array.
     *
     * @param data Metric data byte buffer.
     * @param arr Target byte array.
     * @param off Target byte array offset.
     */
    private static void writeData(VarIntWriter data, byte[] arr, int off) {
        data.toBytes(arr, off);
    }

    /**
     * Writes metrics values to the temporary buffer.
     *
     * @param metrics Metric registries.
     * @return Metric values data set as {@link VarIntWriter} instance.
     */
    private static VarIntWriter generateData(Map<String, MetricRegistry> metrics) {
        VarIntWriter buf = new VarIntWriter(DEFAULT_VARINT_BYTE_BUF_CAPACITY);

        for (Map.Entry<String, MetricRegistry> r : metrics.entrySet()) {
            for (Map.Entry<String, Metric> e : r.getValue().metrics().entrySet()) {
                Metric m = e.getValue();

                MetricType type = MetricType.findByClass(m.getClass());

                // Unsupported type. Just ignore in schema and in data set.
                if (type == null)
                    continue;

                // Most popular metric types are first.
                if (type == LONG)
                    buf.putVarLong(((LongMetric)m).value());
                else if (type == INT)
                    buf.putVarInt(((IntMetric)m).value());
                else if (type == HIT_RATE) {
                    HitRateMetric metric = (HitRateMetric)m;

                    buf.putVarLong(metric.rateTimeInterval());

                    buf.putVarLong(metric.value());
                }
                else if (type == HISTOGRAM) {
                    HistogramMetric metric = (HistogramMetric)m;

                    long[] bounds = metric.bounds();

                    long[] vals = metric.value();

                    buf.putVarInt(bounds.length);

                    // Pairs.
                    for (int i = 0; i < bounds.length; i++) {
                        buf.putVarLong(bounds[i]);

                        buf.putVarLong(vals[i]);
                    }

                    // Infinity value.
                    buf.putVarLong(vals[vals.length - 1]);
                }
                else if (type == DOUBLE)
                    buf.putDouble(((DoubleMetric) m).value());
                else if (type == BOOLEAN)
                    buf.putBoolean(((BooleanMetric)m).value());
                else
                    throw new IllegalStateException("Unknown metric type [metric=" + m + ", type=" + type + ']');
            }
        }

        return buf;
    }
}

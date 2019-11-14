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

package org.apache.ignite.agent.dto.metric;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines metric value types.
 */
public enum MetricType {
    /** Boolean value. */
    BOOLEAN((byte)0, BooleanMetric.class),

    /** Integer value. */
    INT((byte)1, IntMetric.class),

    /** Long value. */
    LONG((byte)2, LongMetric.class),

    /** Double value */
    DOUBLE((byte)3, DoubleMetric.class),

    /**
     * Hit rate value.
     *
     * @see HitRateMetric
     */
    HIT_RATE((byte)4, HitRateMetric.class),

    /**
     * Histogram value.
     *
     * @see HistogramMetric
     */
    HISTOGRAM((byte)5, HistogramMetric.class);

    /** Index by {@code type}. */
    private static final MetricType[] typeIdx = new MetricType[MetricType.values().length];

    /** Mapping by {@link Metric} actual class. */
    private static final Map<Class, MetricType> classIdx = new HashMap<>();

    /** Type as byte. */
    private final byte type;

    /** Corresponding actual class. */
    private final Class<?> cls;

    /**
     * Constructor.
     *
     * @param type Type as byte.
     * @param cls Actual {@link Metric} class.
     */
    MetricType(byte type, Class cls) {
        this.type = type;
        this.cls = cls;
    }

    /**
     * Returns metric type as byte value.
     *
     * @return Metric type byte value.
     */
    public byte type() {
        return type;
    }

    /**
     * Returns corresponding actual class.
     *
     * @return Class corresponding to {@link Metric} implementation.
     */
    public Class clazz() {
        return cls;
    }

    /**
     * Lookups {@link MetricType} instance by type represented byte value.
     *
     * @param type Type byte value.
     * @return {@link MetricType} instance corresponding to given type byte value. Never return {@code null}.
     * @throws IllegalArgumentException for invalid type.
     */
    @NotNull public static MetricType findByType(byte type) {
        if (type < 0 || type >= typeIdx.length)
            throw new IllegalArgumentException("Unknown metric type [type=" + type + ']');

        MetricType res = typeIdx[type];

        if (res == null) {
            for (MetricType m : MetricType.values()) {
                if (m.type() == type) {
                    res = m;

                    typeIdx[type] = m;
                }
            }
        }

        return res;
    }

    /**
     * Returns {@link MetricType} instance accordingly to given class.
     *
     * @param cls Class.
     * @return Corresponding {@link MetricType} instance. Can be {@code null} for unknown or unsupported class.
     */
    @Nullable public static MetricType findByClass(Class<? extends Metric> cls) {
        // HitRateMetric implements LongMetric interface so we need handle this case in the specific manner.
        if (cls.equals(HitRateMetric.class))
            return HIT_RATE;

        // HistogramMetric implements ObjectMetric interface so we need handle this case in the specific manner.
        if (cls.equals(HistogramMetric.class))
            return HISTOGRAM;

        MetricType res = classIdx.get(cls);

        if (res == null) {
            for (MetricType m : MetricType.values()) {
                if (m.cls.isAssignableFrom(cls)) {
                    res = m;

                    classIdx.put(cls, m);
                }
            }
        }

        return res;
    }
}

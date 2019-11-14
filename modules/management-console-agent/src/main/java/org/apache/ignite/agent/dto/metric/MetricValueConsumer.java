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

import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;

/**
 * <p>Visitor interface that should be implemented and passed to
 * {@link MetricResponse#processData(MetricSchema, MetricValueConsumer)} method.</p>
 *
 * <p>Despite the fact that metric types set is more than {@code boolean}, {@code double}, {@code int}, and
 * {@code long}, all complex metrics (e.g. {@link HistogramMetric}) can be represented as set of primitive types.</p>
 */
public interface MetricValueConsumer {
    /**
     * Callback for {@code boolean} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onBoolean(String name, boolean val);

    /**
     * Callback for {@code int} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onInt(String name, int val);

    /**
     * Callback for {@code long} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onLong(String name, long val);

    /**
     * Callback for {@code double} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onDouble(String name, double val);
}

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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.spi.metric.Metric;

/**
 * Metrics representation for a {@link SystemView}.
 */
public class MetricsView {
    /** Metric. */
    private final Metric metric;

    /**
     * @param metric Metric.
     */
    public MetricsView(Metric metric) {
        this.metric = metric;
    }

    /**
     * @return Metric name.
     */
    @Order
    public String name() {
        return metric.name();
    }

    /**
     * @return Metric value.
     */
    @Order(1)
    public String value() {
        return metric.getAsString();
    }

    /**
     * @return Metric description.
     */
    @Order(2)
    public String description() {
        return metric.description();
    }
}

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

package org.apache.ignite.internal.processors.metrics;

import java.util.function.DoubleSupplier;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.gauge.Gauge;

/**
 * Implementation based on primitive supplier.
 */
public class DoubleMetricImpl extends AbstractMetric implements DoubleMetric, Gauge {
    /**
     * Value supplier.
     */
    private final DoubleSupplier value;

    /**
     * @param name Name.
     * @param description Description.
     * @param value Supplier.
     */
    public DoubleMetricImpl(String name, String description, DoubleSupplier value) {
        super(name, description);

        this.value = value;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        try {
            return value.getAsDouble();
        }
        catch (Exception e) {
            e.printStackTrace();

            return 0;
        }
    }
}

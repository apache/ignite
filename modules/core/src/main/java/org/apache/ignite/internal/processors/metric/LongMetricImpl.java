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

package org.apache.ignite.internal.processors.metric;

import java.util.function.LongSupplier;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.gauge.Gauge;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class LongMetricImpl extends AbstractMetric implements LongMetric, Gauge {
    /** Value supplier. */
    private final LongSupplier val;

    /**
     * @param name Name.
     * @param descr Description.
     * @param val Supplier.
     */
    public LongMetricImpl(String name, @Nullable String descr, LongSupplier val) {
        super(name, descr);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val.getAsLong();
    }
}

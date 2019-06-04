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

import java.util.function.Supplier;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.gauge.Gauge;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class ObjectMetricImpl<T> extends AbstractMetric implements ObjectMetric<T>, Gauge {
    /**
     * Value supplier.
     */
    private final Supplier<T> val;

    /**
     * Type.
     */
    private final Class<T> type;

    /**
     * @param name Name.
     * @param descr Description.
     * @param value Supplier.
     * @param type Type.
     */
    public ObjectMetricImpl(String name, @Nullable String descr, Supplier<T> val, Class<T> type) {
        super(name, descr);

        this.val = val;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        try {
            return val.get();
        }
        catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public Class<T> type() {
        return type;
    }
}

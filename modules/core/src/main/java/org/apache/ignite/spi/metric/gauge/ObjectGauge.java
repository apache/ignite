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

package org.apache.ignite.spi.metric.gauge;

import org.apache.ignite.internal.processors.metrics.AbstractMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ObjectMetric}.
 */
public class ObjectGauge<T> extends AbstractMetric implements ObjectMetric<T>, Gauge {
    /**
     * Value.
     */
    private volatile T value;

    /**
     * Type.
     */
    private final Class<T> type;

    /**
     * @param name Name.
     * @param description Description.
     * @param type Type.
     */
    public ObjectGauge(String name, @Nullable String description, Class<T> type) {
        super(name, description);

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return value;
    }

    /** {@inheritDoc} */
    @Override public Class<T> type() {
        return type;
    }

    /**
     * Sets value.
     *
     * @param x Value.
     */
    public void value(T value) {
        this.value = value;
    }
}

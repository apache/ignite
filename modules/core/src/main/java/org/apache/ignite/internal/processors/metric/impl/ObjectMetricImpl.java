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

package org.apache.ignite.internal.processors.metric.impl;

import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ObjectMetric}.
 */
public class ObjectMetricImpl<T> extends AbstractMetric implements ObjectMetric<T> {
    /** Value. */
    private volatile T val;

    /** Type. */
    private final Class<T> type;

    /**
     * @param name Name.
     * @param desc Description.
     * @param type Type.
     */
    public ObjectMetricImpl(String name, @Nullable String desc, Class<T> type) {
        super(name, desc);

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public Class<T> type() {
        return type;
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(T val) {
        this.val = val;
    }
}

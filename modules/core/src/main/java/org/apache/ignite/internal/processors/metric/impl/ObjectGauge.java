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

import java.util.function.Supplier;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class ObjectGauge<T> extends AbstractMetric implements ObjectMetric<T> {
    /** Value supplier. */
    private final Supplier<T> val;

    /** Type. */
    private final Class<T> type;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     * @param type Type.
     */
    public ObjectGauge(String name, @Nullable String desc, Supplier<T> val, Class<T> type) {
        super(name, desc);

        this.val = val;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return val.get();
    }

    /** {@inheritDoc} */
    @Override public Class<T> type() {
        return type;
    }
}

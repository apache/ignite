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

import java.util.function.IntSupplier;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class IntGauge extends AbstractMetric implements IntMetric {
    /** Value supplier. */
    private final IntSupplier val;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     */
    public IntGauge(String name, @Nullable String desc, IntSupplier val) {
        super(name, desc);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return val.getAsInt();
    }
}

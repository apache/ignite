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

package org.apache.ignite.internal.processors.metric.impl;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation based on primitive supplier.
 */
public class LongGauge extends AbstractMetric implements LongMetric {
    /** Value supplier. */
    private final LongSupplier val;

    /**
     * @param name Name.
     * @param desc Description.
     * @param val Supplier.
     */
    public LongGauge(String name, @Nullable String desc, LongSupplier val) {
        super(name, desc);

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val.getAsLong();
    }
}

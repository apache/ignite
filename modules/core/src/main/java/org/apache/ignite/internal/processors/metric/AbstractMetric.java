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

import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for {@code Metric} implementations.
 */
public abstract class AbstractMetric implements Metric {
    /** Name. */
    private final String name;

    /** Description. */
    @Nullable private final String desc;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public AbstractMetric(String name, String desc) {
        assert name != null;
        assert !name.isEmpty();

        this.name = name;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String description() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AbstractMetric metric = (AbstractMetric)o;

        if (!name.equals(metric.name))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }
}

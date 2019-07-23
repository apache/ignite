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

package org.apache.ignite.ml.selection.scoring.metric;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Common interface to present metric values for different ML tasks.
 */
public interface MetricValues {
    /** Returns the pair of metric name and metric value. */
    public default Map<String, Double> toMap() {
        Map<String, Double> metricValues = new HashMap<>();
        Class<? extends MetricValues> aCls = getClass();
        for (Field field : aCls.getDeclaredFields()) {
            try {
                field.setAccessible(true);
                metricValues.put(field.getName(), field.getDouble(this));
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot read field [class=" + aCls.getSimpleName() + ", field_name=" + field.getName() + "]", e);
            }
        }

        return metricValues;
    }
}

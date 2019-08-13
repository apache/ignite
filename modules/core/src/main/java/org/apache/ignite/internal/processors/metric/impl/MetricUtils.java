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

import org.apache.ignite.internal.util.GridStringBuilder;

public class MetricUtils {
    /**
     * Returns histogram represented by json string.
     *
     * @param metric Metric.
     * @return Json.
     */
    public static String toJson(HistogramMetric metric) {
        GridStringBuilder json = new GridStringBuilder();

        json.a("{\"bounds\":[");

        long[] bounds = metric.bounds();

        for (int i = 0; i < bounds.length; i++) {
            json.a(bounds[i]);

            if (i < bounds.length - 1)
                json.a(",");
        }

        json.a("],\"values\":[");

        long[] values = metric.value();

        for (int i = 0; i < values.length; i++) {
            long fromExclusive = (i == 0 ? 0 : bounds[i - 1]);
            long toInclusive = (i == values.length - 1 ? -1 : bounds[i]);
            long val = values[i];

            json.a("{\"fromExclusive\":").a(fromExclusive);

            if (toInclusive >= 0)
                json.a(",\"toInclusive\":").a(toInclusive);

            json
                .a(",\"value\":")
                .a(val)
                .a("}");

            if (i < values.length - 1)
                json.a(",");
        }

        json.a("]}");

        return json.toString();
    }
}

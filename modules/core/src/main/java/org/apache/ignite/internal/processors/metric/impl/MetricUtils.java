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

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Utility class for metrics.
 */
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
            long from = (i == 0 ? 0 : bounds[i - 1]);
            long to = (i == values.length - 1 ? -1 : bounds[i]);
            long val = values[i];

            json.a(i == 0 ? "{\"fromInclusive\":" : "{\"fromExclusive\":").a(from);

            if (to >= 0)
                json.a(",\"toInclusive\":").a(to);

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

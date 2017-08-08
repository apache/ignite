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

package org.apache.ignite.marshaller.estimate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class DataModel {
    /** Estimated class name */
    private String className;

    /** Count of instances to estimate */
    private long count;

    /** Field names mapped to their stats */
    private Map<String, FieldStats> fieldStatsMap = new HashMap<>();

    public String className() {
        return className;
    }

    public DataModel className(String className) {
        this.className = className;

        return this;
    }

    public long count() {
        return count;
    }

    public DataModel count(long count) {
        this.count = count;

        return this;
    }

    public Map<String, FieldStats> fieldStatsMap() {
        return Collections.unmodifiableMap(fieldStatsMap);
    }

    public DataModel fieldStatsMap(Map<String, FieldStats> fieldStatsMap) {
        this.fieldStatsMap = new HashMap<>(fieldStatsMap);

        return this;
    }

    public DataModel setFieldStats(String name, FieldStats stats) {
        fieldStatsMap.put(name, stats);

        return this;
    }

    public DataModel removeFieldStats(String name) {
        fieldStatsMap.remove(name);

        return this;
    }

    public DataModel clearFieldStats() {
        fieldStatsMap.clear();

        return this;
    }

    /**
     *
     */
    public static class FieldStats {

        private Integer nullsPercent;

        private Integer averageSize;

        public Integer nullsPercent() {
            return nullsPercent;
        }

        public FieldStats() {
        }

        public FieldStats(Integer nullsPercent, Integer averageSize) {
            validateNullsPercent(nullsPercent);
            validateAverageSize(averageSize);

            this.nullsPercent = nullsPercent;
            this.averageSize = averageSize;
        }

        public FieldStats nullsPercent(Integer nullsPercent) {
            validateNullsPercent(nullsPercent);

            this.nullsPercent = nullsPercent;

            return this;
        }

        public Integer averageSize() {
            return averageSize;
        }

        public FieldStats averageSize(Integer averageSize) {
            validateAverageSize(averageSize);

            this.averageSize = averageSize;

            return this;
        }

        private void validateNullsPercent(Integer nullsPercent) {
            if (nullsPercent != null) {
                if (nullsPercent < 0 || nullsPercent > 100)
                    throw new IllegalArgumentException("nullsPercent must be null or integer value between 0 and 100 inclusive");
            }
        }

        private void validateAverageSize(Integer averageSize) {
            if (averageSize != null) {
                if (averageSize < 0)
                    throw new IllegalArgumentException("averageSize must be null or integer value greater or equal 0");
            }
        }
    }
}

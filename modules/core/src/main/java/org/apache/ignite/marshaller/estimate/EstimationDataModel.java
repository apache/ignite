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
public class EstimationDataModel {
    /** Estimated class name */
    private String clsName;

    /** Count of instances to estimate */
    private long cnt;

    /** Field names mapped to their stats */
    private Map<String, FieldStats> fieldStatsMap = new HashMap<>();

    /**
     * Field names mapped to their types. If not {@code null}, BinarySample will be created, and ReflectionSample
     * otherwise.
     */
    private Map<String, String> fieldTypes;

    /**
     * @return Estimated class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @param clsName Estimated class name.
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel className(String clsName) {
        this.clsName = clsName;

        return this;
    }

    /**
     * @return Count of instances to estimate.
     */
    public long count() {
        return cnt;
    }

    /**
     * @param cnt Count of instances to estimate.
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel count(long cnt) {
        this.cnt = cnt;

        return this;
    }

    /**
     * @return FieldStats mapped to field names.
     */
    public Map<String, FieldStats> fieldStatsMap() {
        return Collections.unmodifiableMap(fieldStatsMap);
    }

    /**
     * Updates map of FieldStats by externally created map. New map will be created, so further modifications of passed
     * map will not take effect.
     *
     * @param fieldStatsMap new map of FieldStats mapped to field names.
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel fieldStatsMap(Map<String, FieldStats> fieldStatsMap) {
        this.fieldStatsMap = new HashMap<>(fieldStatsMap);

        return this;
    }

    /**
     * Maps new FieldStats instance to field name.
     *
     * @param name Name of field.
     * @param stats FieldStats instance.
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel setFieldStats(String name, FieldStats stats) {
        fieldStatsMap.put(name, stats);

        return this;
    }

    /**
     * @param name Name of field.
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel removeFieldStats(String name) {
        fieldStatsMap.remove(name);

        return this;
    }

    /**
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel clearFieldStats() {
        fieldStatsMap.clear();

        return this;
    }

    /**
     * @return Unmodifiable view of field types mapped to field names.
     */
    public Map<String, String> fieldTypes() {
        return fieldTypes == null ? null : Collections.unmodifiableMap(fieldTypes);
    }

    /**
     * @param name
     * @param typeName
     * @return {@code this} EstimationDataModel instance.
     */
    public EstimationDataModel setFieldType(String name, String typeName) {
        if (fieldTypes == null)
            fieldTypes = new HashMap<>();

        fieldTypes.put(name, typeName);

        return this;
    }

    /**
     *
     */
    public static class FieldStats {
        /** Percent of {@code null} values of related field in sampled objects. */
        private Integer nullsPercent;

        /** Average size of string or array field values in sampled objects. */
        private Integer averageSize;

        /**
         * @return Percent of nulls.
         */
        public Integer nullsPercent() {
            return nullsPercent;
        }

        /**
         * Default constructor.
         */
        public FieldStats() {
        }

        /**
         * @param nullsPercent Nulls percent.
         * @param averageSize Average size.
         */
        public FieldStats(Integer nullsPercent, Integer averageSize) {
            validateNullsPercent(nullsPercent);
            validateAverageSize(averageSize);

            this.nullsPercent = nullsPercent;
            this.averageSize = averageSize;
        }

        /**
         * @param nullsPercent Nulls percent.
         */
        public FieldStats nullsPercent(Integer nullsPercent) {
            validateNullsPercent(nullsPercent);

            this.nullsPercent = nullsPercent;

            return this;
        }

        /**
         * @return Average size of string or array values.
         */
        public Integer averageSize() {
            return averageSize;
        }

        /**
         * @param averageSize Average size.
         */
        public FieldStats averageSize(Integer averageSize) {
            validateAverageSize(averageSize);

            this.averageSize = averageSize;

            return this;
        }

        /**
         * @param nullsPercent Nulls percent.
         */
        private void validateNullsPercent(Integer nullsPercent) {
            if (nullsPercent != null) {
                if (nullsPercent < 0 || nullsPercent > 100)
                    throw new IllegalArgumentException("nullsPercent must be null or integer value between 0 and 100 inclusive");
            }
        }

        /**
         * @param averageSize Average size.
         */
        private void validateAverageSize(Integer averageSize) {
            if (averageSize != null) {
                if (averageSize < 0)
                    throw new IllegalArgumentException("averageSize must be null or integer value greater or equal 0");
            }
        }
    }
}

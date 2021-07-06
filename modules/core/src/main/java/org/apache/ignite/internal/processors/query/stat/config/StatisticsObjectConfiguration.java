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
package org.apache.ignite.internal.processors.query.stat.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Describe configuration of the statistic for a database object (e.g. TABLE).
 */
public class StatisticsObjectConfiguration implements Serializable {
    /** Rows limit to renew partition statistics in percent. */
    public static final byte DEFAULT_OBSOLESCENCE_MAX_PERCENT = 15;

    /** */
    private static final long serialVersionUID = 0L;

    /** Database object key (schema and name). */
    @GridToStringInclude
    private final StatisticsKey key;

    /** Map of the statistic configurations for columns (column_name -> configuration). */
    @GridToStringInclude
    private final Map<String, StatisticsColumnConfiguration> cols;

    /** Max percent of obsolescence row. */
    private final byte maxPartitionObsolescencePercent;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param cols Column statistics configuration.
     * @param maxPartitionObsolescencePercent Maximum number of changed rows per partition.
     */
    public StatisticsObjectConfiguration(
        StatisticsKey key,
        Collection<StatisticsColumnConfiguration> cols,
        byte maxPartitionObsolescencePercent
    ) {
        this.key = key;
        this.cols = (cols == null) ? null : cols.stream()
            .collect(Collectors.toMap(StatisticsColumnConfiguration::name, Function.identity()));
        this.maxPartitionObsolescencePercent = maxPartitionObsolescencePercent;
    }

    /**
     * Merge configuration changes with existing configuration.
     *
     * @param oldCfg Previous configuration.
     * @param newCfg Contains target configuration changes.
     * @return merged configuration.
     */
    public static StatisticsObjectConfiguration merge(
        @NotNull StatisticsObjectConfiguration oldCfg,
        @NotNull StatisticsObjectConfiguration newCfg
    ) {
        assert oldCfg.key.equals(newCfg.key) : "Invalid stat config to merge: [oldKey=" + oldCfg.key
            + ", newKey=" + newCfg.key + ']';

        Map<String, StatisticsColumnConfiguration> cols = new HashMap<>(oldCfg.cols);

        for (StatisticsColumnConfiguration c : newCfg.cols.values())
            cols.put(c.name(), StatisticsColumnConfiguration.merge(cols.get(c.name()), c));

        for (StatisticsColumnConfiguration oldC : oldCfg.cols.values())
            if (!cols.containsKey(oldC.name()))
                cols.put(oldC.name(), oldC);

        return new StatisticsObjectConfiguration(newCfg.key, cols.values(), newCfg.maxPartitionObsolescencePercent);
    }

    /**
     * Creates new configuration object for drop specified columns from current configuration.
     * Marks dropped columns as tombstone.
     *
     * @param dropColNames Set of dropped columns.
     * @return Result configuration object.
     */
    public StatisticsObjectConfiguration dropColumns(Set<String> dropColNames) {
        Map<String, StatisticsColumnConfiguration> newCols = new HashMap<>();

        for (StatisticsColumnConfiguration col : cols.values()) {
            if (F.isEmpty(dropColNames) || dropColNames.contains(col.name()))
                newCols.put(col.name(), col.createTombstone());
            else
                newCols.put(col.name(), col);
        }

        return new StatisticsObjectConfiguration(key, newCols.values(), maxPartitionObsolescencePercent);
    }

    /**
     * Creates new configuration object to refresh statistic with current configuration.
     *
     * @param refreshCols Set of columns to refresh, if {@code null} or empty - all columns will be refreshed.
     * @return Result configuration object.
     */
    public StatisticsObjectConfiguration refresh(Set<String> refreshCols) {
        List<StatisticsColumnConfiguration> newCols;

        if (F.isEmpty(refreshCols))
            newCols = new ArrayList<>(cols.values());
        else {
            newCols = new ArrayList<>(cols.size());
            for (StatisticsColumnConfiguration col : cols.values()) {
                if (refreshCols.contains(col.name()))
                    newCols.add(col.refresh());
                else
                    newCols.add(col);
            }
        }

        return new StatisticsObjectConfiguration(key, newCols, maxPartitionObsolescencePercent);
    }

    /**
     * Calculate diff between two configuration.
     *
     * @param oldCfg Current configuration.
     * @param newCfg Target configuration.
     * @return Configurations differences.
     */
    public static Diff diff(
        StatisticsObjectConfiguration oldCfg,
        StatisticsObjectConfiguration newCfg
    ) {
        if (oldCfg == null)
            return new Diff(Collections.emptySet(), newCfg.cols);

        Set<String> dropCols = new HashSet<>();
        Map<String, StatisticsColumnConfiguration> updateCols = new HashMap<>();

        for (StatisticsColumnConfiguration colNew : newCfg.cols.values()) {
            StatisticsColumnConfiguration colOld = oldCfg.cols.get(colNew.name());

            if (colOld == null || (colNew.version() > colOld.version() && !colNew.tombstone()) ||
                !Objects.equals(colOld.overrides(), colNew.overrides()))
                updateCols.put(colNew.name(), colNew);
            else if (colNew.tombstone() && !colOld.tombstone())
                dropCols.add(colNew.name());
        }

        return new Diff(dropCols, updateCols);
    }

    /**
     * Get database object key (schema and name).
     * @return statistic key.
     */
    public StatisticsKey key() {
        return key;
    }

    /**
     * Get configurations of all statistic columns includes tombstone configuration objects (dropped columns).
     *
     * @return statistic key.
     */
    public Map<String, StatisticsColumnConfiguration> columnsAll() {
        return cols;
    }

    /**
     * Get active (non tombstone) columns statistics configuration.
     *
     * @return Map column name to column statistics configuration.
     */
    public Map<String, StatisticsColumnConfiguration> columns() {
        return (cols == null) ? Collections.emptyMap() : cols.entrySet().stream()
            .filter(e -> !e.getValue().tombstone())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * @return Maximum number of changed rows per partition.
     */
    public byte maxPartitionObsolescencePercent() {
        return maxPartitionObsolescencePercent;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsObjectConfiguration that = (StatisticsObjectConfiguration)o;

        return Objects.equals(key, that.key)
            && Objects.equals(cols, that.cols)
            && maxPartitionObsolescencePercent == that.maxPartitionObsolescencePercent;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(key, cols, maxPartitionObsolescencePercent);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsObjectConfiguration.class, this);
    }

    /**
     * Difference between current and target configuration.
     */
    public static class Diff {
        /**
         * Statistic columns to drop.
         */
        private final Set<String> dropCols;

        /**
         * Statistic columns to update.
         */
        private final Map<String, StatisticsColumnConfiguration> updateCols;

        /** */
        public Diff(
            Set<String> dropCols,
            Map<String, StatisticsColumnConfiguration> updateCols
        ) {
            this.dropCols = dropCols;
            this.updateCols = updateCols;
        }

        /**
         * Statistics columns to drop.
         *
         * @return Set of the columns' names.
         */
        public Set<String> dropCols() {
            return dropCols;
        }

        /**
         * Statistics columns to update.
         *
         * @return Map of the statistic configuration for columns (column_name -> column_configuration).
         */
        public Map<String, StatisticsColumnConfiguration> updateCols() {
            return updateCols;
        }
    }
}

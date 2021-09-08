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

package org.apache.ignite.internal.processors.query.stat.view;

import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Statistics column configuration representation for a {@link SystemView}.
 */
public class StatisticsColumnConfigurationView {
    /** Table object type. */
    public static final String TABLE_TYPE = "TABLE";

    /** Statistics object configuration. */
    private final StatisticsObjectConfiguration objCfg;

    /** Statistics column configuration. */
    private final StatisticsColumnConfiguration colCfg;

    /**
     * Constructor.
     *
     * @param objCfg Statistics object configuration.
     * @param colCfg Statistics object configuration.
     */
    public StatisticsColumnConfigurationView(StatisticsObjectConfiguration objCfg, StatisticsColumnConfiguration colCfg) {
        this.objCfg = objCfg;
        this.colCfg = colCfg;
    }

    /**
     * @return Schema name.
     */
    @Order
    @Filtrable
    public String schema() {
        return objCfg.key().schema();
    }

    /**
     * @return Object type.
     */
    @Order(1)
    @Filtrable
    public String type() {
        return TABLE_TYPE;
    }

    /**
     * @return Object name.
     */
    @Order(2)
    @Filtrable
    public String name() {
        return objCfg.key().obj();
    }

    /**
     * @return Column name.
     */
    @Order(3)
    @Filtrable
    public String column() {
        return colCfg.name();
    }

    @Order(4)
    public byte maxPartitionObsolescencePercent() {
        return objCfg.maxPartitionObsolescencePercent();
    }

    @Order(5)
    public Long manualNulls() {
        return (colCfg.overrides() == null) ? null : colCfg.overrides().nulls();
    }

    @Order(6)
    public Long manualDistinct() {
        return (colCfg.overrides() == null) ? null : colCfg.overrides().distinct();
    }

    @Order(7)
    public Long manualTotal() {
        return (colCfg.overrides() == null) ? null : colCfg.overrides().total();
    }

    @Order(8)
    public Integer manualSize() {
        return (colCfg.overrides() == null) ? null : colCfg.overrides().size();
    }

    /**
     * @return Configuration version.
     */
    @Order(9)
    public long version() {
        return colCfg.version();
    }
}

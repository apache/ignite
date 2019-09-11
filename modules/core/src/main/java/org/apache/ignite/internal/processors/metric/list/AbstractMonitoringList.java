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

package org.apache.ignite.internal.processors.metric.list;

import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;

/**
 *
 */
abstract class AbstractMonitoringList<R extends MonitoringRow> implements MonitoringList<R> {
    /** Name of the list. */
    private final String name;

    /** Description of the list. */
    private final String descr;

    /** Class of the row */
    private final Class<R> rowCls;

    /**
     * Row attribute walker.
     *
     * @see org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator
     */
    private final MonitoringRowAttributeWalker<R> walker;

    /**
     * @param name Name of the list.
     * @param descr Description of the list.
     * @param rowCls Class of the row.
     * @param walker Row attribute walker.
     */
    AbstractMonitoringList(String name, String descr, Class<R> rowCls, MonitoringRowAttributeWalker<R> walker) {
        assert rowCls != null;
        assert walker != null : "Please, add walker class via GridMetricManager#registerWalker";

        this.name = name;
        this.descr = descr;
        this.rowCls = rowCls;
        this.walker = walker;
    }

    /** {@inheritDoc} */
    @Override public MonitoringRowAttributeWalker<R> walker() {
        return walker;
    }

    /** {@inheritDoc} */
    @Override public Class<R> rowClass() {
        return rowCls;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return descr;
    }
}

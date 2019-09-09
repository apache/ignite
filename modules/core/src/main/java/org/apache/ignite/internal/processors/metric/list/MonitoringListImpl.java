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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class to store data for monitoring some internal Ignite objects.
 *
 * @param <Id> Type of the row identificator.
 * @param <R> Type of the row.
 * @see MonitoringList
 */
public class MonitoringListImpl<Id, R extends MonitoringRow<Id>> implements MonitoringList<Id, R> {
    /** Name of the list. */
    private final String name;

    /** Description of the list. */
    private final String description;

    /** Class of the row */
    private final Class<R> rowClass;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Data of the list. */
    private final ConcurrentHashMap<Id, R> data = new ConcurrentHashMap<>();

    /**
     * Row attribute walker.
     *
     * @see org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator
     */
    private final MonitoringRowAttributeWalker<R> walker;

    /**
     * @param name Name of the list.
     * @param description Description of the list.
     * @param rowClass Class of the row.
     * @param walker Row attribute walker.
     * @param log Logger.
     */
    public MonitoringListImpl(String name, String description, Class<R> rowClass, MonitoringRowAttributeWalker<R> walker,
        IgniteLogger log) {
        assert rowClass != null;
        assert walker != null : "Please, add walker class via GridMetricManager#registerWalker";

        this.name = name;
        this.description = description;
        this.rowClass = rowClass;
        this.log = log;
        this.walker = walker;
    }

    /** {@inheritDoc} */
    @Override public MonitoringRowAttributeWalker<R> walker() {
        return walker;
    }

    /** {@inheritDoc} */
    @Override public Class<R> rowClass() {
        return rowClass;
    }

    /**
     * Adds row to the list.
     * This method intentionally created package-private.
     * Please, use {@link ListUtils#addToList(MonitoringListImpl, Supplier)}
     *
     * @param row Row.
     * @see ListUtils#addToList(MonitoringListImpl, Supplier)
     */
    void add(R row) {
        data.put(row.monitoringRowId(), row);
    }

    /**
     * Adds row to the list if not exists.
     * This method intentionally created package-private.
     * Please, use {@link ListUtils#addIfAbsentToList(MonitoringListImpl, Supplier)}
     *
     * @param row Row.
     * @see ListUtils#addIfAbsentToList(MonitoringListImpl, Supplier)
     */
    void addIfAbsent(R row) {
        data.putIfAbsent(row.monitoringRowId(), row);
    }

    /**
     * Removes row from the list.
     * This method intentionally created package-private.
     * Please, use {@link ListUtils#removeFromList(MonitoringListImpl, Object)}
     *
     * @param id Id of the row.
     * @return Removed row.
     * @see ListUtils#removeFromList(MonitoringListImpl, Object)
     */
    R remove(Id id) {
        return data.remove(id);
    }

    /**
     * @param id Idenitificator of the row.
     * @return Row if exists, null otherwise.
     */
    @Nullable public R get(Id id) {
        return data.get(id);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return description;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        return data.values().iterator();
    }

    /** Clears list data. */
    public void clear() {
        data.clear();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data.size();
    }
}

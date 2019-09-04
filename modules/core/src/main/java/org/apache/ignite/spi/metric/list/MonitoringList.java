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

package org.apache.ignite.spi.metric.list;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.notifyListeners;

/**
 * Class to store data for monitoring some internal Ignite objects.
 *
 * @param <Id> Type of the row identificator.
 * @param <R> Type of the row.
 */
public class MonitoringList<Id, R extends MonitoringRow<Id>> implements Iterable<R> {
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

    /** Row creation listeners. */
    private volatile List<Consumer<R>> rowCreationLsnrs;

    /** Row remove listeners. */
    private volatile List<Consumer<R>> rowRemoveLsnrs;

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
    public MonitoringList(String name, String description, Class<R> rowClass, MonitoringRowAttributeWalker<R> walker,
        IgniteLogger log) {
        assert rowClass != null;
        assert walker != null : "Please, add walker class via GridMetricManager#registerWalker";

        this.name = name;
        this.description = description;
        this.rowClass = rowClass;
        this.log = log;
        this.walker = walker;
    }

    /** @return Helper for exporters. */
    public MonitoringRowAttributeWalker<R> walker() {
        return walker;
    }

    /** @return Class of the row. */
    public Class<R> rowClass() {
        return rowClass;
    }

    /**
     * Adds row to the list.
     *
     * @param row Row.
     */
    public void add(R row) {
        data.put(row.monitoringRowId(), row);

        notifyListeners(row, rowCreationLsnrs, log);
    }

    /**
     * Adds row to the list if not exists.
     *
     * @param row Row.
     */
    public void addIfAbsent(R row) {
        MonitoringRow<Id> old = data.putIfAbsent(row.monitoringRowId(), row);

        if (old != null)
            return;

        notifyListeners(row, rowCreationLsnrs, log);
    }

    /**
     * Removes row from the list.
     *
     * @param id Id of the row.
     * @return Removed row.
     */
    public R remove(Id id) {
        R rmv = data.remove(id);

        if (rmv == null)
            return null;

        notifyListeners(rmv, rowRemoveLsnrs, log);

        return rmv;
    }

    /**
     * @param id Idenitificator of the row.
     * @return Row if exists, null otherwise.
     */
    @Nullable public R get(Id id) {
        return data.get(id);
    }

    /** @return List name. */
    public String name() {
        return name;
    }

    /** @return List description. */
    public String description() {
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

    /** @return Size of the list. */
    public int size() {
        return data.size();
    }

    /** Adds row creation listener. */
    public synchronized void addRowCreationListener(Consumer<R> lsnr) {
        if (rowCreationLsnrs == null)
            rowCreationLsnrs = new CopyOnWriteArrayList<>();

        rowCreationLsnrs.add(lsnr);
    }

    /** Adds row remove listener. */
    public synchronized void addRowRemoveListener(Consumer<R> lsnr) {
        if (rowRemoveLsnrs == null)
            rowRemoveLsnrs = new CopyOnWriteArrayList<>();

        rowRemoveLsnrs.add(lsnr);
    }
}

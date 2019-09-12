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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * Monitoring list backed by {@code data} {@link ConcurrentMap}.
 */
public class MonitoringListAdapter<R extends MonitoringRow, D> extends AbstractMonitoringList<R> {
    /** Data backed by this list. */
    private final Collection<D> data;

    /** Row function. */
    private final Function<D, R> rowFunc;

    /**
     * @param name Name.
     * @param description Description.
     * @param rowClass Row class.
     * @param walker Walker.
     * @param data Data.
     * @param rowFunc Row function.
     */
    public MonitoringListAdapter(String name, String description, Class<R> rowClass,
        MonitoringRowAttributeWalker<R> walker, Collection<D> data, Function<D, R> rowFunc) {
        super(name, description, rowClass, walker);

        this.data = data;
        this.rowFunc = rowFunc;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        Iterator<D> data = this.data.iterator();

        return new Iterator<R>() {
            @Override public boolean hasNext() {
                return data.hasNext();
            }

            @Override public R next() {
                return rowFunc.apply(data.next());
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data.size();
    }
}

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

package org.apache.ignite.internal.processors.metric.view;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.ignite.spi.metric.view.SystemView;
import org.apache.ignite.spi.metric.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * System view backed by {@code data} {@link ConcurrentMap}.
 */
public class SystemViewAdapter<R, D> implements SystemView<R> {
    /** Data backed by this list. */
    private final Collection<D> data;

    /** Row function. */
    private final Function<D, R> rowFunc;

    /** Name of the view. */
    private final String name;

    /** Description of the view. */
    private final String desc;

    /** Class of the row */
    private final Class<R> rowCls;

    /**
     * Row attribute walker.
     *
     * @see "org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator"
     */
    private final SystemViewRowAttributeWalker<R> walker;

    /**
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param walker Walker.
     * @param data Data.
     * @param rowFunc Row function.
     */
    public SystemViewAdapter(String name, String desc, Class<R> rowCls,
        SystemViewRowAttributeWalker<R> walker, Collection<D> data, Function<D, R> rowFunc) {
        assert rowCls != null;
        assert walker != null : "Please, add walker class via GridMetricManager#registerWalker";

        this.name = name;
        this.desc = desc;
        this.rowCls = rowCls;
        this.walker = walker;

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
    @Override public SystemViewRowAttributeWalker<R> walker() {
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
        return desc;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data.size();
    }
}

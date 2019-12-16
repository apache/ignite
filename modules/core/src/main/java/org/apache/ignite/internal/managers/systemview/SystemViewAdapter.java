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

package org.apache.ignite.internal.managers.systemview;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * System view backed by {@code data} {@link Collection}.
 */
public class SystemViewAdapter<R, D> extends AbstractSystemView<R> {
    /** Data backed by this view. */
    private Collection<D> data;

    /** Data supplier for the view. */
    private Supplier<Collection<D>> dataSupplier;

    /** Row function. */
    private final Function<D, R> rowFunc;

    /**
     * @param name Name.
     * @param desc Description.
     * @param walker Walker.
     * @param data Data.
     * @param rowFunc Row function.
     */
    public SystemViewAdapter(String name, String desc, SystemViewRowAttributeWalker<R> walker, Collection<D> data,
        Function<D, R> rowFunc) {
        super(name, desc, walker);

        A.notNull(data, "data");

        this.data = data;
        this.rowFunc = rowFunc;
    }

    /**
     * @param name Name.
     * @param desc Description.
     * @param walker Walker.
     * @param dataSupplier Data supplier.
     * @param rowFunc Row function.
     */
    public SystemViewAdapter(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Supplier<Collection<D>> dataSupplier, Function<D, R> rowFunc) {
        super(name, desc, walker);

        A.notNull(dataSupplier, "dataSupplier");

        this.dataSupplier = dataSupplier;
        this.rowFunc = rowFunc;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        Iterator<D> dataIter;

        if (data != null)
            dataIter = data.iterator();
        else
            dataIter = dataSupplier.get().iterator();

        return new Iterator<R>() {
            @Override public boolean hasNext() {
                return dataIter.hasNext();
            }

            @Override public R next() {
                return rowFunc.apply(dataIter.next());
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data == null ? dataSupplier.get().size() : data.size();
    }
}

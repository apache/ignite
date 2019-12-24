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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.NotNull;

/**
 * System view which supports attribute filtering.
 */
public class FiltrableSystemViewAdapter<R, D> extends AbstractSystemView<R> implements FiltrableSystemView<R> {
    /** Data supplier for the view. */
    private Function<Map<String, Object>, Iterable<D>> dataSupplier;

    /** Row function. */
    private final Function<D, R> rowFunc;

    /**
     * @param name Name.
     * @param desc Description.
     * @param walker Walker.
     * @param dataSupplier Data supplier.
     * @param rowFunc Row function.
     */
    public FiltrableSystemViewAdapter(String name, String desc, SystemViewRowAttributeWalker<R> walker,
        Function<Map<String, Object>, Iterable<D>> dataSupplier, Function<D, R> rowFunc) {
        super(name, desc, walker);

        A.notNull(dataSupplier, "dataSupplier");

        this.dataSupplier = dataSupplier;
        this.rowFunc = rowFunc;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator(Map<String, Object> filter) {
        if (filter == null)
            filter = Collections.emptyMap();

        return F.iterator(dataSupplier.apply(filter), rowFunc::apply, true);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<R> iterator() {
        return iterator(Collections.emptyMap());
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(dataSupplier.apply(Collections.emptyMap()).iterator());
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 *
 */
class ConvertingClosableIterator<T, R> implements Iterator<R>, AutoCloseable {
    /** */
    private final Iterator<T> it;

    /** */
    private final Function<T, R> converter;

    /** */
    public ConvertingClosableIterator(Iterator<T> it, Function<T, R> converter) {
        this.it = it;
        this.converter = converter;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean hasNext() {
        return it.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override public R next() {
        return converter.apply(it.next());
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() throws Exception {
        if (it instanceof AutoCloseable)
            ((AutoCloseable) it).close();
    }
}

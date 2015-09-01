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

package org.apache.ignite.internal.util;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;

/**
 * Wrapper used to covert {@link org.apache.ignite.spi.IgniteSpiCloseableIterator} to {@link GridCloseableIterator}.
 */
public class GridSpiCloseableIteratorWrapper<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteSpiCloseableIterator<T> iter;

    /**
     * @param iter Spi iterator.
     */
    public GridSpiCloseableIteratorWrapper(IgniteSpiCloseableIterator<T> iter) {
        assert iter != null;

        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws IgniteCheckedException {
        return iter.next();
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        iter.close();
    }
}
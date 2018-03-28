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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

/** */
public abstract class UpdateSourceIteratorAdapter<T> extends GridCloseableIteratorAdapterEx<T> implements UpdateSourceIterator<T> {
    /** */
    private static final long serialVersionUID = 7261873149950232220L;
    /** */
    private final GridCacheOperation op;
    /** */
    private final GridCloseableIterator<T> delegate;

    /**
     * @param delegate Source iterator.
     */
    public UpdateSourceIteratorAdapter(GridCacheOperation op, GridCloseableIterator<T> delegate) {
        this.op = op;
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public GridCacheOperation operation() {
        return op;
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws IgniteCheckedException {
        return delegate.nextX();
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return delegate.hasNextX();
    }

    /** {@inheritDoc} */
    @Override protected void onRemove() throws IgniteCheckedException {
        delegate.removeX();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() throws IgniteCheckedException {
        delegate.close();
    }
}

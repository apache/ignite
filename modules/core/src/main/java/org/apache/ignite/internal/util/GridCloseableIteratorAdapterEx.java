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

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;

/**
 * Adapter for closeable iterator that can be safely closed concurrently.
 */
public abstract class GridCloseableIteratorAdapterEx<T> extends GridIteratorAdapter<T>
    implements GridCloseableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public final T nextX() throws IgniteCheckedException {
        if (closed.get())
            return null;

        try {
            if (!onHasNext())
                throw new NoSuchElementException();

            return onNext();
        }
        catch (IgniteCheckedException e) {
            if (closed.get())
                return null;
            else
                throw e;
        }
    }

    /**
     * @return Next element.
     * @throws IgniteCheckedException If failed.
     * @throws NoSuchElementException If no element found.
     */
    protected abstract T onNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final boolean hasNextX() throws IgniteCheckedException {
        if (closed.get())
            return false;

        try {
            return onHasNext();
        }
        catch (IgniteCheckedException e) {
            if (closed.get())
                return false;
            else
                throw e;
        }
    }

    /**
     * @return {@code True} if iterator has next element.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract boolean onHasNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final void removeX() throws IgniteCheckedException {
        if (closed.get())
            throw new NoSuchElementException("Iterator has been closed.");

        try {
            onRemove();
        }
        catch (IgniteCheckedException e) {
            if (!closed.get())
                throw e;
        }
    }

    /**
     * Called on remove from iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void onRemove() throws IgniteCheckedException {
        throw new UnsupportedOperationException("Remove is not supported.");
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IgniteCheckedException {
        if (closed.compareAndSet(false, true))
            onClose();
    }

    /**
     * Invoked on iterator close.
     *
     * @throws IgniteCheckedException If closing failed.
     */
    protected void onClose() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed.get();
    }
}
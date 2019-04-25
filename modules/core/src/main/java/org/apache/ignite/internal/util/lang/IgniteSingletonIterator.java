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

package org.apache.ignite.internal.util.lang;

import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;

/**
 * Trivial iterator to return single item.
 */
public class IgniteSingletonIterator<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final T val;

    /** */
    private boolean hasNext = true;

    /** */
    public IgniteSingletonIterator(T val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return hasNext;
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws IgniteCheckedException {
        if (!hasNext)
            throw new NoSuchElementException();

        hasNext = false;

        return val;
    }
}

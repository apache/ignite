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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper wich iterable over the elements of the inner collections.
 *
 * @param <T> Type of the inner collections.
 */
public class GridIteratorOverInnerCollectionsAdapter<T> extends GridIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Iterable<? extends Iterable<T>> iterable;

    /** */
    private Iterator<? extends Iterable<T>> iterator;

    /** */
    private Iterator<T> next;

    /** */
    private boolean moved;

    /** */
    private boolean more;

    /**
     * @param iterable Input collection of collections.
     */
    public GridIteratorOverInnerCollectionsAdapter(Iterable<? extends Iterable<T>> iterable) {
        this.iterable = iterable;
        iterator = iterable.iterator();
        moved = true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() {
        if (!moved)
            return more;

        moved = false;

        if (next != null && next.hasNext())
            return more = true;

        while (iterator.hasNext()) {
            next = iterator.next().iterator();

            if (next.hasNext())
                return more = true;
        }

        return more = false;
    }

    /** {@inheritDoc} */
    @Override public T nextX() {
        if (hasNext()) {
            moved = true;

            return next.next();
        }

        throw new NoSuchElementException();
    }

    /** {@inheritDoc} */
    @Override public void removeX() {
        assert next != null;

        next.remove();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIteratorOverInnerCollectionsAdapter.class, this);
    }
}

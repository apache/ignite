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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Iterator over the elements of given iterators.
 *
 * @param <T> Type of the inner iterators.
 */
public class FlatIterator<T> extends GridIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Iterator<?> iter;

    /** */
    private Iterator<T> next;

    /** */
    private boolean moved;

    /** */
    private boolean more;

    /**
     * @param iterable Input iterable of iterators.
     */
    public FlatIterator(Iterable<?> iterable) {
        iter = iterable.iterator();
        moved = true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean hasNextX() {
        if (!moved)
            return more;

        moved = false;

        if (next != null && next.hasNext())
            return more = true;

        while (iter.hasNext()) {
            Object obj = iter.next();

            if (obj instanceof Iterable)
                next = ((Iterable)obj).iterator();
            else if (obj instanceof Iterator)
                next = (Iterator)obj;
            else
                assert false : "Iterable or Iterator are expected";

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
        return S.toString(FlatIterator.class, this);
    }
}

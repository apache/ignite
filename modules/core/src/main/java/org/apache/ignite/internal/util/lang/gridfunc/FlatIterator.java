/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

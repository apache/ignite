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

package org.apache.ignite.internal.util;


import java.util.AbstractList;

/**
 * List that can contain maximum of one element. Does not allow null element to be added.
 */
public class MutableSingletonList<E> extends AbstractList<E> {

    /** The only element of collection. */
    private E element;

    /** {@inheritDoc} */
    @Override public E get(int index) {
        if (index != 0 || element == null)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());

        return element;
    }

    /** {@inheritDoc} */
    @Override public void add(int index, E element) {
        if (element == null)
            throw new IllegalArgumentException("Cannot add null element to list");
        else if (index != 0)
            throw new IllegalStateException("Element already added to singleton list");
        else
            this.element = element;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return element == null ? 0 : 1;
    }
}

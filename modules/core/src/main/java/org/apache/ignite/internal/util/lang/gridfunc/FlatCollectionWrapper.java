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

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.util.GridSerializableCollection;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper which iterable over the elements of the inner collections.
 *
 * @param <T> Type of the inner collections.
 */
public class FlatCollectionWrapper<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Collection<? extends Collection<T>> cols;

    /**
     * @param cols Input collection of collections.
     */
    public FlatCollectionWrapper(Collection<? extends Collection<T>> cols) {
        this.cols = cols;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return GridFunc.flat((Iterable<? extends Iterable<T>>)cols);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(iterator());
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return !iterator().hasNext();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FlatCollectionWrapper.class, this);
    }
}

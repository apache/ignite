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

package org.apache.ignite.internal.util.lang;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;

/**
 * Trivial iterator to wrap around another one.
 */
public class IgniteCloseableIteratorAdapter<T> extends GridCloseableIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Iterator<T> iter;

    /** */
    public IgniteCloseableIteratorAdapter(Iterable<T> iterable) {
        this(iterable.iterator());
    }

    /** */
    public IgniteCloseableIteratorAdapter(Iterator<T> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override protected T onNext() throws IgniteCheckedException {
        if (!onHasNext())
            throw new NoSuchElementException();

        return iter.next();
    }
}
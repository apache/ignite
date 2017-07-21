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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

/**
 * Weak iterator.
 */
public class GridWeakIterator<T> extends WeakReference<Iterator<T>> {
    /** Nested closeable iterator. */
    private final GridCloseableIterator<T> it;

    /**
     * @param ref Referent.
     * @param it Closeable iterator.
     * @param q Referent queue.
     */
    public GridWeakIterator(Iterator<T> ref, GridCloseableIterator<T> it,
        ReferenceQueue<Iterator<T>> q) {
        super(ref, q);

        assert it != null;

        this.it = it;
    }

    /**
     * Closes iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void close() throws IgniteCheckedException {
        it.close();
    }
}
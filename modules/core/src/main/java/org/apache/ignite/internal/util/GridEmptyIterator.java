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
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Empty iterator.
 */
public class GridEmptyIterator<T> extends GridIteratorAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean hasNextX() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public T nextX() {
        throw new NoSuchElementException("Iterator is empty.");
    }

    /** {@inheritDoc} */
    @Override public void removeX() {
        throw new NoSuchElementException("Iterator is empty.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEmptyIterator.class, this);
    }
}

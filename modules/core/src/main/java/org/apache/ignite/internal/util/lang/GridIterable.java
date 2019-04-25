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

import java.util.Iterator;

/**
 * Defines "rich" iterable interface that is also acts as lambda function and iterator.
 * @see GridIterator
 */
public interface GridIterable<T> extends GridIterator<T> {
    /**
     * Returns {@link GridIterator} which extends regular {@link Iterator} interface
     * and adds methods that account for possible failures in cases when iterating
     * over data that has been partially received over network.
     *
     * @return Instance of new {@link GridIterator}.
     */
    @Override public GridIterator<T> iterator();
}
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

package org.apache.ignite.cache.query.index;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Cursor that holds single value only.
 * @param <T> class of value to return.
 */
public class SingleCursor<T> implements GridCursor<T> {
    /** Value to return */
    private final T val;

    /** Counter ot check wherther value is already got. */
    private final AtomicInteger currIdx = new AtomicInteger(-1);

    /** */
    public SingleCursor(T val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        return currIdx.incrementAndGet() == 0;
    }

    /** {@inheritDoc} */
    @Override public T get() throws IgniteCheckedException {
        if (currIdx.get() == 0)
            return val;

        throw new IgniteCheckedException();
    }
}

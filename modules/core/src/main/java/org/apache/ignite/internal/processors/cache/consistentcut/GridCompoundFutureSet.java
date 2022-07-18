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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;

/** Compound future that doesn't allow duplicated futures to insert. */
public class GridCompoundFutureSet<T, R> extends GridCompoundFuture<T, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Collection<IgniteInternalFuture> createFuturesCollection() {
        return new HashSet<>();
    }

    /** */
    public boolean containsFuture(IgniteInternalFuture f) {
        if (futs == null)
            return false;

        if (futs instanceof IgniteInternalFuture)
            return futs.equals(f);

        return ((Set)futs).contains(f);
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<T> future(int idx) {
        throw new IllegalStateException("Must not be invoked");
    }
}

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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Deserializes portable objects if needed.
 */
public class GridQueryCacheObjectsIterator implements Iterator<List<?>>, AutoCloseable {
    /** */
    private final Iterator<List<?>> iter;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final boolean keepPortable;

    /**
     * @param iter Iterator.
     * @param cctx Cache context.
     * @param keepPortable Keep portable.
     */
    public GridQueryCacheObjectsIterator(Iterator<List<?>> iter, GridCacheContext<?,?> cctx, boolean keepPortable) {
        this.iter = iter;
        this.cctx = cctx;
        this.keepPortable = keepPortable;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (iter instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)iter);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<?> next() {
        return (List<?>)cctx.unwrapPortablesIfNeeded((Collection<Object>)iter.next(), keepPortable);
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        iter.remove();
    }
}
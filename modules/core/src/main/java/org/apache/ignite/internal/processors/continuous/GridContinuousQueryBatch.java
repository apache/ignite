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

package org.apache.ignite.internal.processors.continuous;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntry;

/**
 * Continuous routine batch adapter.
 */
public class GridContinuousQueryBatch extends GridContinuousBatchAdapter {
    /** Entries size included filtered entries. */
    private final AtomicInteger size = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void add(Object obj) {
        assert obj != null;
        assert obj instanceof CacheContinuousQueryEntry || obj instanceof List;

        if (obj instanceof CacheContinuousQueryEntry) {
            buf.add(obj);

            size.incrementAndGet();
        }
        else {
            List<Object> objs = (List<Object>)obj;

            buf.addAll(objs);

            size.addAndGet(objs.size());
        }
    }

    /**
     * @return Entries count.
     */
    public int entriesCount() {
        return size.get();
    }
}

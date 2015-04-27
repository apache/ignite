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

package org.apache.ignite.memory;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.*;
import org.apache.ignite.internal.processors.query.h2.opt.*;
import org.ehcache.sizeof.*;
import org.ehcache.sizeof.filters.*;
import org.h2.index.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Utility class that will estimate memory occupied by cache.
 */
public class MemoryEstimator {
    /** Default cache size sampling. */
    private static final int DFLT_CACHE_SIZE_SAMPLING = 10;

    /** */
    private final SizeOf sizeOfCache;
    /** */
    private final SizeOf sizeOfIndex1;

    private final Set<String> ignored = new HashSet<>();
//    /** */
//    private final SizeOf sizeOfIndex2;

    /**
     * Default constructor.
     */
    public MemoryEstimator() {
        ignored.add("org.h2.engine.Database");
        ignored.add("org.h2.message.TraceSystem");

        SizeOfFilter filter1 = new SizeOfFilter() {
            @Override public Collection<Field> filterFields(Class<?> klazz, Collection<Field> fields) {
                return fields;
            }

            @Override public boolean filterClass(Class<?> klazz) {
                String klazzName = klazz.getName();

                System.out.println("]]] " + klazzName);

                return true;
            }
        };

        SizeOfFilter filter = new SizeOfFilter() {
            @Override public Collection<Field> filterFields(Class<?> klazz, Collection<Field> fields) {
                return fields;
            }

            @Override public boolean filterClass(Class<?> klazz) {
                String klazzName = klazz.getName();

                boolean f = (!klazzName.startsWith("org.apache.ignite") ||
                    klazzName.contains("h2") || klazzName.contains("H2") ||
                    klazzName.contains("snaptree")) &&
                    !ignored.contains(klazzName);

                System.out.println("))) " + klazzName + " " + f);

                return f;
            }
        };

        sizeOfCache = SizeOf.newInstance(filter1);
        sizeOfIndex1 = SizeOf.newInstance(filter);
    }

    /**
     * Estimate memory size for entries in cache.
     *
     * @param ca Cache to estimate.
     * @param sample Number of entries to sample.
     * @return Memory size in bytes under cache entries.
     */
    public long estimateCacheSize(GridCacheAdapter ca, int sample) {
        int size = ca.size();

        int cnt = Math.min(sample > 0 ? sample : DFLT_CACHE_SIZE_SAMPLING, size);

        long memSz = 0;

        if (cnt > 0) {
            for (int i = 0; i < cnt; i++) {
                GridCacheMapEntry entry = ca.randomInternalEntry();

                long ksz = sizeOfCache.deepSizeOf(Integer.MAX_VALUE, false, entry.key()).getCalculated();
                long vsz = sizeOfCache.deepSizeOf(Integer.MAX_VALUE, false, entry.rawGet()).getCalculated();

                memSz += (ksz + vsz);
            }

            memSz = (long)((double)memSz / cnt * size);
        }

        return memSz;
    }

    /**
     * Estimate memory size for cache indexes.
     *
     * @param ignite Ignite.
     * @param ca Cache to estimate.
     * @return Memory size in bytes under indexes.
     */
    public long estimateIndexesSize(IgniteEx ignite, GridCacheAdapter ca) {
        long memSz = 0;

        GridQueryIndexing indexing = ignite.context().query().indexing();

        if (indexing instanceof IgniteH2Indexing) {
            IgniteH2Indexing h2Indexing = (IgniteH2Indexing)indexing;

            String cacheName = ca.name();

            if (cacheName == null)
                cacheName = "";

            Collection<TableDescriptor> tbls = h2Indexing.tables(cacheName);

            for (TableDescriptor tbl : tbls) {
                GridH2Table h2Tbl = tbl.h2Table();

                Iterator<Index> idxs = h2Tbl.getIndexes().iterator();

                // Skip first index, as it is special wrapper.
                if (idxs.hasNext())
                    idxs.next();

                while (idxs.hasNext()) {
                    Index idx = idxs.next();

                    long idxSz = sizeOfIndex1.deepSizeOf(Integer.MAX_VALUE, false, idx).getCalculated();

                    System.out.println("Index=" + idx + ", size=" + idxSz);

                    memSz += idxSz;
                }
            }
        }

        return memSz;
    }
}

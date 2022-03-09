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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCompartorImpl;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.cache.query.index.SortOrder.DESC;

/**
 * Reducer for {@code IndexQuery} results.
 */
public class IndexQueryReducer<R> extends MergeSortCacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future that will be completed with first page response. */
    private final CompletableFuture<IndexQueryResultMeta> metaFut;

    /** */
    private final String valType;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** */
    public IndexQueryReducer(
        final String valType,
        final Map<UUID, NodePageStream<R>> pageStreams,
        final GridCacheContext<?, ?> cctx,
        final CompletableFuture<IndexQueryResultMeta> meta
    ) {
        super(pageStreams);

        this.valType = valType;
        this.metaFut = meta;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Comparator<NodePage<R>>> pageComparator() {
        return metaFut.thenApply(m -> {
            LinkedHashMap<String, IndexKeyDefinition> keyDefs = m.keyDefinitions();

            GridQueryTypeDescriptor typeDesc = cctx.kernalContext().query().typeDescriptor(cctx.name(), QueryUtils.typeName(valType));

            return new IndexedNodePageComparator(m, typeDesc, keyDefs);
        });
    }

    /** Comparing rows by indexed keys. */
    private class IndexedNodePageComparator implements Comparator<NodePage<R>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Index key defintiions in case of IndexQuery. */
        private final LinkedHashMap<String, IndexKeyDefinition> keyDefs;

        /** Description of value type for IndexQuery. */
        private final GridQueryTypeDescriptor typeDesc;

        /** IndexQuery meta. */
        private final IndexQueryResultMeta meta;

        /** Every node will return the same key types for the same index, then it's possible to use simple comparator. */
        private final IndexRowComparator idxRowComp = new IndexRowCompartorImpl(null);

        /** */
        IndexedNodePageComparator(
            IndexQueryResultMeta meta,
            GridQueryTypeDescriptor typeDesc,
            LinkedHashMap<String, IndexKeyDefinition> keyDefs
        ) {
            this.meta = meta;
            this.typeDesc = typeDesc;
            this.keyDefs = keyDefs;
        }

        /** {@inheritDoc} */
        @Override public int compare(NodePage<R> o1, NodePage<R> o2) {
            IgniteBiTuple<?, ?> e1 = (IgniteBiTuple<?, ?>)o1.head();
            IgniteBiTuple<?, ?> e2 = (IgniteBiTuple<?, ?>)o2.head();

            Iterator<Map.Entry<String, IndexKeyDefinition>> defs = keyDefs.entrySet().iterator();

            try {
                while (defs.hasNext()) {
                    Map.Entry<String, IndexKeyDefinition> d = defs.next();

                    IndexKey k1 = key(d.getKey(), d.getValue().idxType(), e1);
                    IndexKey k2 = key(d.getKey(), d.getValue().idxType(), e2);

                    int cmp = idxRowComp.compareKey(k1, k2);

                    if (cmp != 0)
                        return d.getValue().order().sortOrder() == DESC ? -cmp : cmp;
                }

                return 0;

            } catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to sort remote index rows", e);
            }
        }

        /** */
        private IndexKey key(String key, int type, IgniteBiTuple<?, ?> entry) throws IgniteCheckedException {
            Object o;

            if (isKeyField(key))
                o = entry.getKey();
            else if (isValueField(key))
                o = entry.getValue();
            else {
                GridQueryProperty prop = typeDesc.property(key);

                o = prop.value(entry.getKey(), entry.getValue());
            }

            return IndexKeyFactory.wrap(o, type, cctx.cacheObjectContext(), meta.keyTypeSettings());
        }

        /** */
        private boolean isKeyField(String fld) {
            return fld.equals(typeDesc.keyFieldAlias()) || fld.equals(QueryUtils.KEY_FIELD_NAME);
        }

        /** */
        private boolean isValueField(String fld) {
            return fld.equals(typeDesc.valueFieldAlias()) || fld.equals(QueryUtils.VAL_FIELD_NAME);
        }
    }
}

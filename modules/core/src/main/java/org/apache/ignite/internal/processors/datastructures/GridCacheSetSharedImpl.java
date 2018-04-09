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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.CacheWeakQueryIteratorsHolder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;

/**
 * Grid cache set implementation for shared cache mode.
 *
 * @param <T> The type of elements maintained by this set.
 */
public class GridCacheSetSharedImpl<T> extends GridCacheSet<T> {
    /**
     * @param ctx Cache context.
     * @param name Set name.
     * @param hdr Set header.
     */
    public GridCacheSetSharedImpl(GridCacheContext ctx, String name, GridCacheSetHeader hdr) {
        super(ctx, name, hdr);
    }

    /**
     * @return Closeable iterator.
     */
    @SuppressWarnings("unchecked")
    protected GridCloseableIterator<T> closeableIterator() {
        try {
            CacheQuery qry = new GridCacheQueryAdapter<>(context(), SET, null, null,
                new GridSetQueryPredicate<>(id(), collocated()), null, false, false);

            Collection<ClusterNode> nodes = dataNodes(context().affinity().affinityTopologyVersion());

            qry.projection(context().grid().cluster().forNodes(nodes));

            CacheQueryFuture<Map.Entry<T, ?>> fut = qry.execute();

            CacheWeakQueryIteratorsHolder.WeakReferenceCloseableIterator it =
                context().itHolder().iterator(fut, new CacheIteratorConverter<T, Map.Entry<T, ?>>() {
                    @Override protected T convert(Map.Entry<T, ?> e) {
                        return e.getKey();
                    }

                    @Override protected void remove(T item) {
                        GridCacheSetSharedImpl.this.remove(item);
                    }
                });

            if (rmvd) {
                context().itHolder().removeIterator(it);

                checkRemoved();
            }

            return it;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public int size() {
        try {
            onAccess();

            if (context().isLocal() || context().isReplicated()) {
                GridConcurrentHashSet<SetItemKey> set = context().dataStructures().setData(id());

                return set != null ? set.size() : 0;
            }

            CacheQuery qry = new GridCacheQueryAdapter<>(context(), SET, null, null,
                new GridSetQueryPredicate<>(id(), collocated()), null, false, false);

            Collection<ClusterNode> nodes = dataNodes(context().affinity().affinityTopologyVersion());

            qry.projection(context().grid().cluster().forNodes(nodes));

            Iterable<Integer> col = (Iterable<Integer>)qry.execute(new SumReducer()).get();

            int sum = 0;

            for (Integer val : col)
                sum += val;

            return sum;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param topVer Topology version.
     * @return Nodes where set data request should be sent.
     * @throws IgniteCheckedException If all cache nodes left grid.
     */
    @SuppressWarnings("unchecked")
    private Collection<ClusterNode> dataNodes(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (context().isLocal() || context().isReplicated())
            return Collections.singleton(context().localNode());

        Collection<ClusterNode> nodes;

        if (collocated()) {
            List<ClusterNode> nodes0 = context().affinity().nodesByPartition(hdrPart, topVer);

            nodes = !nodes0.isEmpty() ?
                Collections.singleton(
                    nodes0.contains(context().localNode()) ? context().localNode() : F.first(nodes0)) : nodes0;
        }
        else
            nodes = CU.affinityNodes(context(), topVer);

        if (nodes.isEmpty())
            throw new IgniteCheckedException("Failed to get set data, all cache nodes left grid.");

        return nodes;
    }

    /**
     *
     */
    private static class SumReducer implements IgniteReducer<Object, Integer>, Externalizable {
        /** */
        private static final long serialVersionUID = -3436987759126521204L;

        /** */
        private int cntr;

        /**
         * Required by {@link Externalizable}.
         */
        public SumReducer() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Object o) {
            cntr++;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return cntr;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}

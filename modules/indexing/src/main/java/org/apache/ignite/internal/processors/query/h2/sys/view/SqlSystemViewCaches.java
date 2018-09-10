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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: caches.
 */
public class SqlSystemViewCaches extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewCaches(GridKernalContext ctx) {
        super("CACHES", "Ignite caches", ctx, "NAME",
            newColumn("NAME"),
            newColumn("IS_USER", Value.BOOLEAN),
            newColumn("GROUP_ID", Value.INT),
            newColumn("GROUP_NAME"),
            newColumn("CACHE_MODE"),
            newColumn("ATOMICITY_MODE"),
            newColumn("DATA_REGION_NAME")
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        Collection<IgniteInternalCache<?, ?>> caches;

        if (nameCond.isEquality()) {
            IgniteInternalCache<?, ?> cache = ctx.cache().cache(nameCond.valueForEquality().getString());

            caches = cache == null ? Collections.emptySet() : Collections.singleton(cache);
        }
        else
            caches = ctx.cache().caches();

        AtomicLong rowKey = new AtomicLong();

        return F.iterator(caches,
            cache -> createRow(ses, rowKey.incrementAndGet(),
                cache.name(),
                cache.context().userCache(),
                cache.context().groupId(),
                cache.context().group().name(),
                cache.configuration().getCacheMode(),
                cache.configuration().getAtomicityMode(),
                cache.context().dataRegion().config().getName()),
            true);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().caches().size();
    }
}

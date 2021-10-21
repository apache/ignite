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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Registry of the running queries.
 */
public class QueryRegistryImpl<RowT> implements QueryRegistry<RowT> {
    /** */
    private final ConcurrentMap<UUID, Query<RowT>> runningQrys = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    public QueryRegistryImpl(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Query<RowT> register(Query<RowT> qry) {
        Query<RowT> old = runningQrys.putIfAbsent(qry.id(), qry);

        return old != null ? old : qry;
    }

    /** {@inheritDoc} */
    @Override public Query<RowT> query(UUID id) {
        return runningQrys.get(id);
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID id) {
        runningQrys.remove(id);
    }

    /** {@inheritDoc} */
    @Override public Collection<Query<RowT>> runningQueries() {
        return runningQrys.values();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        runningQrys.values().forEach(q -> IgniteUtils.close(q::cancel, log));

        runningQrys.clear();
    }
}

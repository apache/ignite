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

package org.apache.ignite.tests.load.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.tests.load.Worker;
import org.apache.ignite.tests.utils.TestsHelper;

/**
 * Cassandra direct load tests worker for bulk read operation CacheStore.load
 */
public class BulkReadWorker extends Worker {
    /** */
    public static final String LOGGER_NAME = "CassandraBulkReadLoadTest";

    /** */
    private List<Object> keys = new ArrayList<>(TestsHelper.getBulkOperationSize());

    /** */
    public BulkReadWorker(CacheStore cacheStore, long startPosition, long endPosition) {
        super(cacheStore, startPosition, endPosition);
    }

    /** {@inheritDoc} */
    @Override protected String loggerName() {
        return LOGGER_NAME;
    }

    /** {@inheritDoc} */
    @Override protected boolean batchMode() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void process(CacheStore cacheStore, Collection<CacheEntryImpl> entries) {
        keys.clear();

        for (CacheEntryImpl entry : entries)
            keys.add(entry.getKey());

        cacheStore.loadAll(keys);
    }
}

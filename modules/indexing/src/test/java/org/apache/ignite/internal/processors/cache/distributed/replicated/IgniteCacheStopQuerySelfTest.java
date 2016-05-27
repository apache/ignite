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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.springframework.util.ReflectionUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests replicated query.
 */
public class IgniteCacheStopQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    @Override protected int gridCount() {
        return 1;
    }

    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * Tests stopping local query.
     */
    public void testRemoteQueryStop() throws Exception {
        try(Ignite client = startGrid("client")) {

            int keyCnt = 10_000;

            IgniteCache<Object, Object> cache1 = client.cache(null);

            assertEquals(0, cache1.localSize());

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            assertEquals(0, cache1.localSize(ALL));

            QueryCursor<Cache.Entry<Integer, String>> qry =
                cache1.query(new SqlQuery<Integer, String>(String.class, "true"));

            // Initiate remote query by requesting the iterator.
            Iterator<Cache.Entry<Integer, String>> iterator = qry.iterator();

            qry.close();
        }
    }
}
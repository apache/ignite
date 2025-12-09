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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.CLIENT;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.SERVER;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_JDBC;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_VIA_CACHE_API;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_VIA_QUERY;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.QUERY;

/** */
public class ScanQueryTransactionIsolationTest extends AbstractQueryTransactionIsolationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected User select(Integer id, ModifyApi api, ExecutorType type, CacheMode mode) {
        assertTrue(type != THIN_VIA_CACHE_API);
        assertTrue(type != THIN_JDBC);

        if (api == QUERY) {
            ScanQuery<Integer, User> qry = new ScanQuery<Integer, User>()
                .setFilter((id0, user) -> Objects.equals(id0, id));

            boolean withTrasformer = (type == SERVER || type == CLIENT) && ThreadLocalRandom.current().nextBoolean();
            boolean useGetAll = ThreadLocalRandom.current().nextBoolean();
            boolean useCacheIter = (type == SERVER || type == CLIENT) && ThreadLocalRandom.current().nextBoolean();

            if (!withTrasformer) {
                if (useCacheIter) {
                    assertTrue(type == SERVER || type == CLIENT);

                    List<Cache.Entry<Integer, User>> res =
                        toList(F.iterator0(node(type).cache(users(mode)), true, e -> Objects.equals(e.getKey(), id)));

                    assertTrue(F.size(res) + "", F.size(res) <= 1);

                    return F.isEmpty(res) ? null : res.get(0).getValue();
                }
                else {
                    QueryCursor<Cache.Entry<Integer, User>> cursor = null;

                    if (type == THIN_VIA_QUERY)
                        cursor = thinCli.<Integer, User>cache(users(mode)).query(qry);
                    else if (type == SERVER || type == CLIENT)
                        cursor = node(type).<Integer, User>cache(users(mode)).query(qry);
                    else
                        fail("Unsupported executor type: " + type);

                    List<Cache.Entry<Integer, User>> res = toList(cursor, useGetAll);

                    assertTrue("useGetAll=" + useGetAll + ", useCacheIter=" + useCacheIter, F.size(res) <= 1);

                    return F.isEmpty(res) ? null : res.get(0).getValue();
                }
            }
            else {
                assertTrue(type == SERVER || type == CLIENT);

                List<User> res = toList(node(type).<Integer, User>cache(users(mode)).query(qry, Cache.Entry::getValue), useGetAll);

                assertTrue("withTransformer=" + withTrasformer + ", useGetAll=" + useGetAll, F.size(res) <= 1);

                return F.first(res);
            }
        }

        return super.select(id, api, type, mode);
    }

    /** */
    private static <R> List<R> toList(QueryCursor<R> cursor, boolean useGetAll) {
        return useGetAll ? cursor.getAll() : toList(cursor.iterator());
    }

    /** */
    private static <R> List<R> toList(Iterator<R> iter) {
        List<R> res = new ArrayList<>();

        iter.forEachRemaining(res::add);

        return res;
    }
}

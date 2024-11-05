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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.CLIENT;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.SERVER;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_JDBC;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_VIA_CACHE_API;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_VIA_QUERY;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.QUERY;

/** */
public class ScanQueryTransactionIsolationTest extends AbstractQueryTransactionIsolationTest {
    /** @return Test parameters. */
    @Parameterized.Parameters(
        name = "gridCnt={0},backups={1},partitionAwareness={2},mode={3},execType={4},modify={5},commit={6},multi={7},txConcurrency={8}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (int gridCnt : new int[]{1, 3}) {
            int[] backups = gridCnt > 1
                ? new int[]{1, gridCnt - 1}
                : new int[]{0};

            for (int backup : backups) {
                for (CacheMode mode : CacheMode.values()) {
                    for (ModifyApi modify : new ModifyApi[]{ModifyApi.CACHE, ModifyApi.ENTRY_PROCESSOR}) {
                        for (boolean commit : new boolean[]{false, true}) {
                            for (boolean mutli : new boolean[]{false, true}) {
                                for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                                    for (ExecutorType execType : new ExecutorType[]{SERVER, ExecutorType.CLIENT}) {
                                        params.add(new Object[]{
                                            gridCnt,
                                            backup,
                                            false, //partition awareness
                                            mode,
                                            execType,
                                            modify,
                                            commit,
                                            mutli,
                                            txConcurrency
                                        });
                                    }

                                    for (boolean partitionAwareness : new boolean[]{false, true}) {
                                        params.add(new Object[]{
                                            gridCnt,
                                            backup,
                                            partitionAwareness,
                                            mode,
                                            THIN_VIA_QUERY, // executor type
                                            modify,
                                            commit,
                                            mutli,
                                            txConcurrency
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected User select(Integer id, ModifyApi api) {
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
                        toList(F.iterator0(node().cache(users()), true, e -> Objects.equals(e.getKey(), id)));

                    assertTrue(F.size(res) + "", F.size(res) <= 1);

                    return F.isEmpty(res) ? null : res.get(0).getValue();
                }
                else {
                    QueryCursor<Cache.Entry<Integer, User>> cursor = null;

                    if (type == THIN_VIA_QUERY)
                        cursor = thinCli.<Integer, User>cache(users()).query(qry);
                    else if (type == SERVER || type == CLIENT) {
                        cursor = node().<Integer, User>cache(users()).query(qry);
                    }
                    else
                        fail("Unsupported executor type: " + type);

                    List<Cache.Entry<Integer, User>> res = toList(cursor, useGetAll);

                    assertTrue("useGetAll=" + useGetAll + ", useCacheIter=" + useCacheIter, F.size(res) <= 1);

                    return F.isEmpty(res) ? null : res.get(0).getValue();
                }
            }
            else {
                assertTrue(type == SERVER || type == CLIENT);

                List<User> res = toList(node().<Integer, User>cache(users()).query(qry, Cache.Entry::getValue), useGetAll);

                assertTrue("withTransformer=" + withTrasformer + ", useGetAll=" + useGetAll, F.size(res) <= 1);

                return F.first(res);
            }

        }

        return super.select(id, api);
    }

    private static <R> List<R> toList(QueryCursor<R> cursor, boolean useGetAll) {
        return useGetAll ? cursor.getAll() : toList(cursor.iterator());
    }

    private static <R> List<R> toList(Iterator<R> iter) {
        List<R> res = new ArrayList<>();

        iter.forEachRemaining(res::add);

        return res;
    }

    /** */
    private List<Cache.Entry<Integer, User>> unwrapBinary(List<?> all) {
        return all.stream()
            .map(e0 -> new CacheEntryImpl<>(
                this.<Integer>unwrap(((Cache.Entry<?, ?>)e0).getKey()),
                this.<User>unwrap(((Cache.Entry<?, ?>)e0).getValue()))
            ).collect(Collectors.toList());
    }

    /** */
    private <T> T unwrap(Object o) {
        if (o instanceof KeyCacheObject)
            return ((CacheObject)o).value(null, false);
        else if (o instanceof BinaryObject) {
            return ((BinaryObject)o).deserialize();
        }
        return (T)o;
    }
}

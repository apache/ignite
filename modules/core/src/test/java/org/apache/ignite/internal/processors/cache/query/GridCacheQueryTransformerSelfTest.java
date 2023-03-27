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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SpiQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for scan query with transformer.
 */
public class GridCacheQueryTransformerSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);
        startClientGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetKeys() throws Exception {
        IgniteCache<Integer, String> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, "val" + i);

            IgniteClosure<Cache.Entry<Integer, String>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, String>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, String> e) {
                        return e.getKey();
                    }
                };

            List<Integer> keys = cache.query(new ScanQuery<Integer, String>(), transformer).getAll();

            assertEquals(50, keys.size());

            Collections.sort(keys);

            for (int i = 0; i < 50; i++)
                assertEquals(i, keys.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetKeysFiltered() throws Exception {
        IgniteCache<Integer, String> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, "val" + i);

            IgniteBiPredicate<Integer, String> filter = new IgniteBiPredicate<Integer, String>() {
                @Override public boolean apply(Integer k, String v) {
                    return k % 10 == 0;
                }
            };

            IgniteClosure<Cache.Entry<Integer, String>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, String>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, String> e) {
                        return e.getKey();
                    }
                };

            List<Integer> keys = cache.query(new ScanQuery<>(filter), transformer).getAll();

            assertEquals(5, keys.size());

            Collections.sort(keys);

            for (int i = 0; i < 5; i++)
                assertEquals(i * 10, keys.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetObjectField() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                        return e.getValue().idx;
                    }
                };

            List<Integer> res = cache.query(new ScanQuery<Integer, Value>(), transformer).getAll();

            assertEquals(50, res.size());

            Collections.sort(res);

            for (int i = 0; i < 50; i++)
                assertEquals(i * 100, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetObjectFieldPartitioned() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        Affinity<Integer> affinity = affinity(cache);

        try {
            int[] keys = new int[50];

            for (int i = 0, j = 0; i < keys.length; j++) {
                if (affinity.partition(j) == 0)
                    keys[i++] = j;
            }

            for (int i : keys)
                cache.put(i, new Value("str" + i, i * 100));

            IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                        return e.getValue().idx;
                    }
                };

            List<Integer> res = cache.query(new ScanQuery<Integer, Value>().setPartition(0), transformer).getAll();

            assertEquals(50, res.size());

            Collections.sort(res);

            for (int i = 0; i < keys.length; i++)
                assertEquals(keys[i] * 100, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetObjectFieldFiltered() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            IgniteBiPredicate<Integer, Value> filter = new IgniteBiPredicate<Integer, Value>() {
                @Override public boolean apply(Integer k, Value v) {
                    return v.idx % 1000 == 0;
                }
            };

            IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                        return e.getValue().idx;
                    }
                };

            List<Integer> res = cache.query(new ScanQuery<>(filter), transformer).getAll();

            assertEquals(5, res.size());

            Collections.sort(res);

            for (int i = 0; i < 5; i++)
                assertEquals(i * 1000, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testKeepBinary() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

            IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, BinaryObject> e) {
                        return e.getValue().field("idx");
                    }
                };

            List<Integer> res = binaryCache.query(new ScanQuery<Integer, BinaryObject>(), transformer).getAll();

            assertEquals(50, res.size());

            Collections.sort(res);

            for (int i = 0; i < 50; i++)
                assertEquals(i * 100, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testKeepBinaryFiltered() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

            IgniteBiPredicate<Integer, BinaryObject> filter = new IgniteBiPredicate<Integer, BinaryObject>() {
                @Override public boolean apply(Integer k, BinaryObject v) {
                    return v.<Integer>field("idx") % 1000 == 0;
                }
            };

            IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, BinaryObject> e) {
                        return e.getValue().field("idx");
                    }
                };

            List<Integer> res = binaryCache.query(new ScanQuery<>(filter), transformer).getAll();

            assertEquals(5, res.size());

            Collections.sort(res);

            for (int i = 0; i < 5; i++)
                assertEquals(i * 1000, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocal() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            Collection<List<Integer>> lists = grid().compute().broadcast(new IgniteCallable<List<Integer>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public List<Integer> call() throws Exception {
                    IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                        new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                            @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                                return e.getValue().idx;
                            }
                        };

                    return ignite.cache("test-cache").query(new ScanQuery<Integer, Value>().setLocal(true),
                        transformer).getAll();
                }
            });

            List<Integer> res = new ArrayList<>(F.flatCollections(lists));

            assertEquals(50, res.size());

            Collections.sort(res);

            for (int i = 0; i < 50; i++)
                assertEquals(i * 100, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalFiltered() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            Collection<List<Integer>> lists = grid().compute().broadcast(new IgniteCallable<List<Integer>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public List<Integer> call() throws Exception {
                    IgniteBiPredicate<Integer, Value> filter = new IgniteBiPredicate<Integer, Value>() {
                        @Override public boolean apply(Integer k, Value v) {
                            return v.idx % 1000 == 0;
                        }
                    };

                    IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                        new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                            @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                                return e.getValue().idx;
                            }
                        };

                    return ignite.cache("test-cache").query(new ScanQuery<>(filter).setLocal(true),
                        transformer).getAll();
                }
            });

            List<Integer> res = new ArrayList<>(F.flatCollections(lists));

            assertEquals(5, res.size());

            Collections.sort(res);

            for (int i = 0; i < 5; i++)
                assertEquals(i * 1000, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalKeepBinary() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            Collection<List<Integer>> lists = grid().compute().broadcast(new IgniteCallable<List<Integer>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public List<Integer> call() throws Exception {
                    IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer> transformer =
                        new IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer>() {
                            @Override public Integer apply(Cache.Entry<Integer, BinaryObject> e) {
                                return e.getValue().field("idx");
                            }
                        };

                    return ignite.cache("test-cache").withKeepBinary().query(
                        new ScanQuery<Integer, BinaryObject>().setLocal(true), transformer).getAll();
                }
            });

            List<Integer> res = new ArrayList<>(F.flatCollections(lists));

            assertEquals(50, res.size());

            Collections.sort(res);

            for (int i = 0; i < 50; i++)
                assertEquals(i * 100, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalKeepBinaryFiltered() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            Collection<List<Integer>> lists = grid().compute().broadcast(new IgniteCallable<List<Integer>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public List<Integer> call() throws Exception {
                    IgniteBiPredicate<Integer, BinaryObject> filter = new IgniteBiPredicate<Integer, BinaryObject>() {
                        @Override public boolean apply(Integer k, BinaryObject v) {
                            return v.<Integer>field("idx") % 1000 == 0;
                        }
                    };

                    IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer> transformer =
                        new IgniteClosure<Cache.Entry<Integer, BinaryObject>, Integer>() {
                            @Override public Integer apply(Cache.Entry<Integer, BinaryObject> e) {
                                return e.getValue().field("idx");
                            }
                        };

                    return ignite.cache("test-cache").withKeepBinary().query(new ScanQuery<>(filter).setLocal(true),
                        transformer).getAll();
                }
            });

            List<Integer> res = new ArrayList<>(F.flatCollections(lists));

            assertEquals(5, res.size());

            Collections.sort(res);

            for (int i = 0; i < 5; i++)
                assertEquals(i * 1000, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnsupported() throws Exception {
        final IgniteCache<Integer, Integer> cache = grid().createCache("test-cache");

        final IgniteClosure<Cache.Entry<Integer, Integer>, Integer> transformer =
            new IgniteClosure<Cache.Entry<Integer, Integer>, Integer>() {
                @Override public Integer apply(Cache.Entry<Integer, Integer> e) {
                    return null;
                }
            };

        try {
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.query(new SqlQuery<Integer, Integer>(Integer.class, "clause"), transformer);

                        return null;
                    }
                },
                UnsupportedOperationException.class,
                "Transformers are supported only for SCAN queries."
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.query(new SqlFieldsQuery("clause"), new IgniteClosure<List<?>, Object>() {
                            @Override public Object apply(List<?> objects) {
                                return null;
                            }
                        });

                        return null;
                    }
                },
                UnsupportedOperationException.class,
                "Transformers are supported only for SCAN queries."
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.query(new TextQuery<Integer, Integer>(Integer.class, "clause"), transformer);

                        return null;
                    }
                },
                UnsupportedOperationException.class,
                "Transformers are supported only for SCAN queries."
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.query(new SpiQuery<Integer, Integer>(), transformer);

                        return null;
                    }
                },
                UnsupportedOperationException.class,
                "Transformers are supported only for SCAN queries."
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.query(new ContinuousQuery<Integer, Integer>(), transformer);

                        return null;
                    }
                },
                UnsupportedOperationException.class,
                "Transformers are supported only for SCAN queries."
            );
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageSize() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        int numEntries = 10_000;
        int pageSize = 3;

        try {
            for (int i = 0; i < numEntries; i++)
                cache.put(i, new Value("str" + i, i));

            IgniteClosure<Cache.Entry<Integer, Value>, Integer> transformer =
                new IgniteClosure<Cache.Entry<Integer, Value>, Integer>() {
                    @Override public Integer apply(Cache.Entry<Integer, Value> e) {
                        return e.getValue().idx;
                    }
                };

            ScanQuery<Integer, Value> query = new ScanQuery<>();
            query.setPageSize(pageSize);

            List<Integer> res = cache.query(query, transformer).getAll();

            assertEquals(numEntries, res.size());

            Collections.sort(res);

            for (int i = 0; i < numEntries; i++)
                assertEquals(i, res.get(i).intValue());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalInjection() throws Exception {
        IgniteCache<Integer, Value> cache = grid().createCache("test-cache");

        try {
            for (int i = 0; i < 50; i++)
                cache.put(i, new Value("str" + i, i * 100));

            Collection<List<Boolean>> lists = grid().compute().broadcast(new IgniteCallable<List<Boolean>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public List<Boolean> call() throws Exception {
                    IgniteClosure<Cache.Entry<Integer, Value>, Boolean> transformer =
                        new IgniteClosure<Cache.Entry<Integer, Value>, Boolean>() {
                            @IgniteInstanceResource
                            Ignite ignite;

                            @Override public Boolean apply(Cache.Entry<Integer, Value> e) {
                                return ignite != null;
                            }
                        };

                    return ignite.cache("test-cache").query(new ScanQuery<Integer, Value>().setLocal(true),
                        transformer).getAll();
                }
            });

            List<Boolean> res = new ArrayList<>(F.flatCollections(lists));

            assertEquals(50, res.size());

            for (int i = 0; i < 50; i++)
                assertEquals(Boolean.TRUE, res.get(i));
        }
        finally {
            cache.destroy();
        }
    }

    /**
     */
    private static class Value {
        /** */
        @SuppressWarnings("unused")
        private String str;

        /** */
        private int idx;

        /**
         * @param str String.
         * @param idx Integer.
         */
        public Value(String str, int idx) {
            this.str = str;
            this.idx = idx;
        }
    }
}

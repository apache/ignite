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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.transactions.TransactionChanges;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class KeyFilteringCursorTest {
    /** */
    @Test
    public void testSimple() throws Exception {
        assertEquals(Collections.emptyList(), all(cursor(), keys()));

        assertEquals(Arrays.asList(1, 3), all(cursor(1, 2, 3), keys(2)));

        assertEquals(Arrays.asList(1, 2, 3), all(cursor(1, 2, 3), keys()));

        assertEquals(Collections.emptyList(), all(cursor(1, 2, 3), keys(1, 2, 3)));

        assertEquals(Arrays.asList(1, 2, 3), all(cursor(1, 2, 3), keys(4, 5, 6)));

        assertEquals(Arrays.asList(2, 3), all(cursor(1, 2, 3), keys(1, 4, 5, 6)));

        assertEquals(Collections.emptyList(), all(cursor(1, 2, 3), keys(1, 2, 3, 4, 5, 6)));

        assertEquals(Collections.singletonList(2), all(cursor(1, 2, 3, 4, 5, 6), keys(1, 3, 4, 5, 6)));

        assertEquals(Arrays.asList(1, 2, 3, 4), all(cursor(1, 2, 3, 4, 5, 6), keys(5, 6)));
    }

    /** */
    private List<Integer> all(GridCursor<IndexRow> rawCursor, Set<KeyCacheObject> skipKeys) throws IgniteCheckedException {
        GridCursor<IndexRow> cursor = new KeyFilteringCursor<>(
            rawCursor,
            new TransactionChanges<>(skipKeys, Collections.emptyList()),
            r -> r.cacheDataRow().key()
        );

        List<Integer> res = new ArrayList<>();

        while (cursor.next())
            res.add((Integer)cursor.get().key(0).key());

        return res;
    }

    /** */
    private GridCursor<IndexRow> cursor(int...nums) {
        List<IndexRow> rows = new ArrayList<>();

        for (int num : nums) {
            rows.add(new IndexRow() {
                @Override public IndexKey key(int idx) {
                    return new IntegerIndexKey(num);
                }

                @Override public int keysCount() {
                    return 1;
                }

                @Override public long link() {
                    throw new UnsupportedOperationException();
                }

                @Override public InlineIndexRowHandler rowHandler() {
                    return new InlineIndexRowHandler() {
                        @Override public IndexKey indexKey(int idx, CacheDataRow row) {
                            assert idx == 0;

                            return new IntegerIndexKey(num);
                        }

                        @Override public List<InlineIndexKeyType> inlineIndexKeyTypes() {
                            throw new UnsupportedOperationException();
                        }

                        @Override public List<IndexKeyDefinition> indexKeyDefinitions() {
                            throw new UnsupportedOperationException();
                        }

                        @Override public IndexKeyTypeSettings indexKeyTypeSettings() {
                            throw new UnsupportedOperationException();
                        }

                        @Override public int partition(CacheDataRow row) {
                            return row.partition();
                        }

                        @Override public Object cacheKey(CacheDataRow row) {
                            return row.key();
                        }

                        @Override public Object cacheValue(CacheDataRow row) {
                            return row.value();
                        }
                    };
                }

                @Override public CacheDataRow cacheDataRow() {
                    return new CacheDataRowAdapter(
                        KeyFilteringCursorTest.this.key(num),
                        null,
                        new GridCacheVersion(1, 1, 1L),
                        CU.EXPIRE_TIME_ETERNAL
                    );
                }

                @Override public boolean indexPlainRow() {
                    return false;
                }
            });
        }

        return new SortedListRangeCursor<>(null, rows, null, null, true, true);
    }

    /** */
    private Set<KeyCacheObject> keys(int...nums) {
        return Arrays.stream(nums).mapToObj(this::key).collect(Collectors.toSet());
    }

    /** */
    private KeyCacheObjectImpl key(int num) {
        return new KeyCacheObjectImpl(num, null, -1);
    }
}

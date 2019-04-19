/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * Base class for MVCC continuous queries.
 */
public abstract class CacheMvccAbstractSqlContinuousQuerySelfTest extends CacheMvccAbstractContinuousQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected void cachePut(IgniteCache cache, Integer key, Integer val) {
        cache.query(new SqlFieldsQuery("MERGE INTO Integer (_key, _val) values (" + key + ',' + val + ')')).getAll();
    }

    /** {@inheritDoc} */
    @Override protected void cacheRemove(IgniteCache  cache, Integer key) {
        cache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE _key=" + key)).getAll();
    }
}

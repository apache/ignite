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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class IgniteDbMemoryLeakSqlQueryTest extends IgniteDbMemoryLeakTest {
    /** {@inheritDoc} */
    @Override protected boolean indexingEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long pagesMax() {
        return 26_000;
    }

    /** {@inheritDoc} */
    @Override protected void operation(IgniteCache<Object, Object> cache) {
        Object key = key();
        Object val = value(key);

        switch (nextInt(4)) {
            case 0:
                cache.getAndPut(key, val);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.getAndRemove(key);

                break;

            case 3:
                cache.query(sqlQuery(cache)).getAll();
        }
    }

    /**
     * @param cache IgniteCache.
     * @return SqlFieldsQuery.
     */
    @NotNull private SqlFieldsQuery sqlQuery(IgniteCache<Object, Object> cache) {
        String qry = String.format("select _key from \"%s\".DbValue where iVal=?", cache.getName());

        SqlFieldsQuery sqlQry = new SqlFieldsQuery(qry);
        sqlQry.setArgs(nextInt(200_000));

        return sqlQry;
    }
}

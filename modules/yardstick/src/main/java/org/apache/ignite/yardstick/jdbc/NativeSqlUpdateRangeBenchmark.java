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

package org.apache.ignite.yardstick.jdbc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;

/**
 * Native sql benchmark that performs update operations
 */
public class NativeSqlUpdateRangeBenchmark extends AbstractNativeBenchmark {
    /**
     * Benchmarked action that performs updates (single row or batch)
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long expRsSize;

        SqlFieldsQuery qry;

        if (args.sqlRange() <= 0) {

            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1)");

            expRsSize = args.range();
        }
        else if (args.sqlRange() == 1) {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id = ?");

            qry.setArgs(ThreadLocalRandom.current().nextLong(args.range()) + 1);

            expRsSize = 1;
        }
        else {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?");

            long id = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange()) + 1;
            long maxId = id + args.sqlRange() - 1;

            qry.setArgs(id, maxId);

            expRsSize = args.sqlRange();
        }

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
                .querySqlFieldsNoCache(qry, false)) {
            Iterator<List<?>> it = cursor.iterator();

            List<?> cntRow = it.next();

            long rsSize = (Long)cntRow.get(0);

            if (it.hasNext())
                throw new Exception("Only one row expected on UPDATE query");

            if (rsSize != expRsSize)
                throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');
        }

        return true;
    }
}

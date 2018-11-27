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

package org.apache.ignite.yardstick.jdbc.vendors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SelectByPkBenchmark extends BaseSelectRangeBenchmark {
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        PreparedStatement select0 = select.get();

        long minId = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange() + 1);
        long maxId = minId + args.sqlRange() - 1;

        select0.setLong(1, minId);
        select0.setLong(2, maxId);

        long rsCnt = 0;

        try (ResultSet res = select0.executeQuery()) {
            while (res.next())
                rsCnt++;
        }

        if (rsCnt != args.sqlRange())
            throw new AssertionError("Server returned wrong number of lines: " +
                "[expected=" + args.sqlRange() + ", actual=" + rsCnt + "].");

        return true;

    }

    @Override protected String testedSqlQuery() {
        return queries.selectPersonsByPK();
    }
}

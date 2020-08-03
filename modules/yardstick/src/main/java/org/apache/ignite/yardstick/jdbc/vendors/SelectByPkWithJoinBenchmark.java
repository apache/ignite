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
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Benchmark that performs selects with filter by primary key field with join on Organization table.
 */
public class SelectByPkWithJoinBenchmark extends BaseSelectRangeBenchmark {
    /** {@inheritDoc} */
    @Override protected void fillTestedQueryParams(PreparedStatement select) throws SQLException {
        long persId = ThreadLocalRandom.current().nextLong(args.range());

        select.setLong(1, persId);
    }

    /** {@inheritDoc} */
    @Override protected String testedSqlQuery() {
        return queries.selectPersonsJoinOrgWherePersonPK();
    }
}

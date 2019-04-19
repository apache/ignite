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

package org.apache.ignite.yardstick.jdbc.vendors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;

/**
 * Benchmark that fetches all the rows from table of Person inner join Organization table. Specify in the properties
 * file {@link IgniteBenchmarkArguments#sqlRange()} to be equal to whole Person table size ({@link
 * IgniteBenchmarkArguments#range()}) since Person table contains more rows than Organization.
 */
public class ScanAllWithJoinBenchmark extends BaseSelectRangeBenchmark {
    /** {@inheritDoc} */
    @Override protected void fillTestedQueryParams(PreparedStatement select) throws SQLException {
        //No-op, query doesn't have parameters to fill.
    }

    /** {@inheritDoc} */
    @Override protected String testedSqlQuery() {
        return queries.selectAllPersonsJoinOrg();
    }
}

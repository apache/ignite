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

/**
 * Benchmark that performs select with filter by 'salary' field.
 */
public class SelectBySalaryBenchmark extends BaseSelectRangeBenchmark {
    /** {@inheritDoc} */
    @Override protected void fillTestedQueryParams(PreparedStatement select) throws SQLException {
        fillRandomSalaryRange(select);
    }

    /** {@inheritDoc} */
    @Override protected String testedSqlQuery() {
        return queries.selectPersonsWithSalaryBetween();
    }
}

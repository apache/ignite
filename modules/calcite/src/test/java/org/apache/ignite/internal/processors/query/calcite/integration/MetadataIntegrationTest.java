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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.generate;

/**
 *
 */
public class MetadataIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void trimColumnNames() {
        createAndPopulateTable();

        String X300 = generate(() -> "X").limit(300).collect(joining());
        String X256 = "'" + X300.substring(0, 255);

        assertQuery("select '" + X300 + "' from person").columnNames(X256).check();
    }

    /** */
    @Test
    public void columnNames() {
        createAndPopulateTable();

        assertQuery("select count(_key), _key from person group by _key")
            .columnNames("COUNT(_KEY)", "_KEY")
            .check();

        assertQuery("select (select count(*) from person), (select avg(salary) from person) from person")
            .columnNames("EXPR$0", "EXPR$1").check();
        assertQuery("select (select count(*) from person) as subquery from person")
            .columnNames("SUBQUERY").check();

        assertQuery("select salary*2, salary/2, salary+2, salary-2, mod(salary, 2)  from person")
            .columnNames("SALARY * 2", "SALARY / 2", "SALARY + 2", "SALARY - 2", "MOD(SALARY, 2)").check();
        assertQuery("select salary*2 as first, salary/2 as secOND from person").columnNames("FIRST", "SECOND").check();

        assertQuery("select trim(name) tr_name from person").columnNames("TR_NAME").check();
        assertQuery("select trim(name) from person").columnNames("TRIM(BOTH ' ' FROM NAME)").check();
        assertQuery("select row(1), ceil(salary), floor(salary), position('text' IN salary) from person")
            .columnNames("ROW(1)", "CEIL(SALARY)", "FLOOR(SALARY)", "POSITION('text' IN SALARY)").check();

        assertQuery("select count(*) from person").columnNames("COUNT(*)").check();
        assertQuery("select count(name) from person").columnNames("COUNT(NAME)").check();
        assertQuery("select max(salary) from person").columnNames("MAX(SALARY)").check();
        assertQuery("select min(salary) from person").columnNames("MIN(SALARY)").check();
        assertQuery("select aVg(salary) from person").columnNames("AVG(SALARY)").check();
        assertQuery("select sum(salary) from person").columnNames("SUM(SALARY)").check();

        assertQuery("select salary, count(name) from person group by salary").columnNames("SALARY", "COUNT(NAME)").check();

        assertQuery("select 1, -1, 'some string' from person").columnNames("1", "-1", "'some string'").check();
    }
}

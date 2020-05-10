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
package org.apache.ignite.springdata.repository.query;

import org.junit.Test;
import org.springframework.data.domain.Sort;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IgniteQueryGeneratorTest {
    @Test
    public void testAddSortingNullsFirst() {
        assertThat(
                IgniteQueryGenerator.addSorting(
                        new StringBuilder("SELECT * FROM someTable"),
                        new Sort(new Sort.Order(Sort.Direction.ASC, "someColumn").nullsFirst())
                ).toString(),
                is("SELECT * FROM someTable ORDER BY someColumn ASC NULLS FIRST")
        );
    }

    @Test
    public void testAddSortingNullsLast() {
        assertThat(
                IgniteQueryGenerator.addSorting(
                        new StringBuilder("SELECT * FROM someTable"),
                        new Sort(new Sort.Order(Sort.Direction.ASC, "someColumn").nullsLast())
                ).toString(),
                is("SELECT * FROM someTable ORDER BY someColumn ASC NULLS LAST")
        );
    }
}

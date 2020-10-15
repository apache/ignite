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
package org.apache.ignite.snippets;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Indexes_groups {

    //tag::group-indexes[]
    public class Person implements Serializable {
        /** Indexed in a group index with "salary". */
        @QuerySqlField(orderedGroups = { @QuerySqlField.Group(name = "age_salary_idx", order = 0, descending = true) })

        private int age;

        /** Indexed separately and in a group index with "age". */
        @QuerySqlField(index = true, orderedGroups = { @QuerySqlField.Group(name = "age_salary_idx", order = 3) })
        private double salary;
    }
    //end::group-indexes[]
}

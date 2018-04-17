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

package org.apache.ignite.sqltests.datamodel;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Employee implements Serializable {
    private static final long serialVersionUID = 7527792073507989754L;

    @QuerySqlField
    private String firstName;

    @QuerySqlField
    private String lastName;

    @QuerySqlField
    private Long id;

    @QuerySqlField
    private Long depId;

    @QuerySqlField
    private Integer age;

    public Employee(Long id, Long depId, String firstName, String lastName, Integer age) {
        this.id = id;
        this.depId = depId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }
}

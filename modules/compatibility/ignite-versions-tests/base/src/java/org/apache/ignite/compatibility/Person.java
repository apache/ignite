/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** Person ID (indexed). */
    @QuerySqlField(index = true)
    private int id;

    /** First name (not-indexed). */
    @QuerySqlField
    private String firstName;

    /** Last name (not indexed). */
    @QuerySqlField
    private String lastName;

    /** Salary (indexed). */
    @QuerySqlField
    private double salary;

    /**
     */
    private Person() {
        // No-op.
    }

    /**
     * @param id Id.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     */
    public Person(int id, String firstName, String lastName, double salary) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [id=" + id +
            ", firstName=" + firstName +
            ", lastName=" + lastName +
            ", salary=" + salary + ']';
    }
}

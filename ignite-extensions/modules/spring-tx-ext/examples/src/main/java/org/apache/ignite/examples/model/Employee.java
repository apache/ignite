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

package org.apache.ignite.examples.model;

import java.util.Collection;

/**
 * This class represents employee object.
 */
public class Employee {
    /** Name. */
    private String name;

    /** Salary. */
    private long salary;

    /** Address. */
    private Address addr;

    /** Departments. */
    private Collection<String> departments;

    /**
     * Required for binary deserialization.
     */
    public Employee() {
        // No-op.
    }

    /**
     * @param name Name.
     * @param salary Salary.
     * @param addr Address.
     * @param departments Departments.
     */
    public Employee(String name, long salary, Address addr, Collection<String> departments) {
        this.name = name;
        this.salary = salary;
        this.addr = addr;
        this.departments = departments;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Salary.
     */
    public long salary() {
        return salary;
    }

    /**
     * @return Address.
     */
    public Address address() {
        return addr;
    }

    /**
     * @return Departments.
     */
    public Collection<String> departments() {
        return departments;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Employee [name=" + name +
            ", salary=" + salary +
            ", address=" + addr +
            ", departments=" + departments + ']';
    }
}

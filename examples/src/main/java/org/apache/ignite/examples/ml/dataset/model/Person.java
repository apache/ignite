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

package org.apache.ignite.examples.ml.dataset.model;

/** Person model. */
public class Person {
    /** Name. */
    private final String name;

    /** Age. */
    private final double age;

    /** Salary. */
    private final double salary;

    /**
     * Constructs a new instance of person.
     *
     * @param name Name.
     * @param age Age.
     * @param salary Salary.
     */
    public Person(String name, double age, double salary) {
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public double getAge() {
        return age;
    }

    /** */
    public double getSalary() {
        return salary;
    }
}

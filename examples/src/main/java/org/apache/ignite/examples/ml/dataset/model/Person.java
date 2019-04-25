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

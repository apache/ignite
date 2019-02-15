/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tests.p2p.cache;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 *
 */
public class Person implements Serializable {
    /** */
    @QuerySqlField
    private String name;

    /** */
    @QuerySqlField(index = true)
    private int id;

    /** */
    @QuerySqlField
    private String lastName;

    /** */
    @QuerySqlField
    private double salary;

    /**
     *
     */
    public Person() {
        // No-op.
    }

    /**
     * @param name Name.
     */
    public Person(String name) {
        this.name = name;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return ID.
     */
    public int getId() {
        return id;
    }

    /**
     * @param id ID.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName Last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Salary.
     */
    public double getSalary() {
        return salary;
    }

    /**
     * @param salary Salary.
     */
    public void setSalary(double salary) {
        this.salary = salary;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Person person = (Person)o;

        if (id != person.id)
            return false;
        if (Double.compare(person.salary, salary) != 0)
            return false;
        if (name != null ? !name.equals(person.name) : person.name != null)
            return false;
        return lastName != null ? lastName.equals(person.lastName) : person.lastName == null;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res;
        long temp;
        res = name != null ? name.hashCode() : 0;
        res = 31 * res + id;
        res = 31 * res + (lastName != null ? lastName.hashCode() : 0);
        temp = Double.doubleToLongBits(salary);
        res = 31 * res + (int)(temp ^ (temp >>> 32));
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person{" +
            "name='" + name + '\'' +
            ", id=" + id +
            ", lastName='" + lastName + '\'' +
            ", salary=" + salary +
            '}';
    }
}
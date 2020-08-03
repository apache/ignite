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

package org.apache.ignite.springdata.misc;

import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

/**
 * DTO class.
 */
public class Person {
    /** First name. */
    @QuerySqlField(index = true)
    @QueryTextField
    private String firstName;

    /** Second name. */
    @QuerySqlField(index = true)
    private String secondName;

    /**
     * @param firstName First name.
     * @param secondName Second name.
     */
    public Person(String firstName, String secondName) {
        this.firstName = firstName;
        this.secondName = secondName;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName First name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Second name.
     */
    public String getSecondName() {
        return secondName;
    }

    /**
     * @param secondName Second name.
     */
    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person{" +
            "firstName='" + firstName + '\'' +
            ", secondName='" + secondName + '\'' +
            '}';
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Person person = (Person)o;

        return Objects.equals(firstName, person.firstName) &&
            Objects.equals(secondName, person.secondName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(firstName, secondName);
    }
}

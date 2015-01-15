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

package org.gridgain.examples.datagrid.store;

import java.io.*;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** Person ID. */
    private long id;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /**
     *
     */
    public Person() {
        // No-op.
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param firstName First name.
     * @param lastName Last name.
     */
    public Person(long id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * @return Person ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id Person ID.
     */
    public void setId(long id) {
        this.id = id;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [id=" + id +
            ", firstName=" + firstName +
            ", lastName=" + lastName + ']';
    }
}

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

package org.apache.ignite.scalar.examples.model

import java.lang.Long

/**
 * Person class.
 */
@SerialVersionUID(0L)
class Person() {
    /** ID field */
    private[this] var id = 0L

    /** ID field */
    private[this] var orgId = 0L

    /** First name field */
    private[this] var firstName: String = null

    /** Last name field */
    private[this] var lastName: String = null

    private[this] var salary = 0.0

    private[this] var resume: String = null

    def this(id: Long, firstName: String, lastName: String) = {
        this()

        this.id = id
        this.firstName = firstName
        this.lastName = lastName
    }

    def this(id: Long, orgId: Long, firstName: String, lastName: String, salary: Double, resume: String) = {
        this()

        this.id = id
        this.orgId = orgId
        this.firstName = firstName
        this.lastName = lastName
        this.salary = salary
        this.resume = resume
    }

    /**
     * @return ID field.
     */
    def getId = id

    /**
     * Update ID field.
     *
     * @param id New ID field.
     */
    def setId(id: Long) {
        this.id = id
    }

    /**
     * @return Org ID field.
     */
    def getOrgId = orgId

    /**
     * Update org ID field.
     *
     * @param orgId New orgID field.
     */
    def setOrgId(orgId: Long) {
        this.orgId = orgId
    }

    /**
     * @return First name field.
     */
    def getFirstName = firstName

    /**
     * Update first name field.
     *
     * @param firstName New first name field.
     */
    def setFirstName(firstName: String) {
        this.firstName = firstName
    }

    /**
     * @return Last name field.
     */
    def getLastName = lastName

    /**
     * Update last name field.
     *
     * @param lastName New last name field.
     */
    def setLastName(lastName: String) {
        this.lastName = lastName
    }

    /**
     * @return Salary field.
     */
    def getSalary = salary

    /**
     * Update salary field.
     *
     * @param salary New salary field.
     */
    def setSalary(salary: Double) {
        this.salary = salary
    }

    /**
      * @return Resume field.
      */
    def getResume = resume

    /**
      * Update resume field.
      *
      * @param resume New resume field.
      */
    def setResume(resume: String) {
        this.resume = resume
    }

    /**
     * `toString` implementation.
     */
    override def toString: String =
        "Person [id=" + id +
            ", orgId=" + orgId +
            ", firstName=" + firstName +
            ", lastName=" + lastName +
            ", salary=" + salary +
            ", resume=" + resume +
            "]"
}

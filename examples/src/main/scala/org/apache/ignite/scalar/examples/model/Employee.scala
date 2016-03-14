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

import org.apache.ignite.examples.model.Address

/**
 * Person class.
 */
@SerialVersionUID(0L)
class Employee() {
    /** Name. */
    private[this] var name: String = null

    /** Salary. */
    private[this] var salary = 0L

    /** Address. */
    private var addr: Address = null

    /** Departments. */
    private var departments: Seq[String] = null

    def this(name: String, salary: Long, addr: Address, departments: Seq[String]) = {
        this()

        this.name = name
        this.salary = salary
        this.addr = addr
        this.departments = departments
    }

    /**
     * @return Name field.
     */
    def getName = name

    /**
     * Update name field.
     *
     * @param name New name field.
     */
    def setName(name: String) {
        this.name = name
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
    def setSalary(salary: Long) {
        this.salary = salary
    }

    /**
     * @return Address field.
     */
    def getAddress = addr

    /**
     * Update address field.
     *
     * @param address New salary field.
     */
    def setAddress(address: Address) {
        this.addr = address
    }

    /**
     * @return Departments field.
     */
    def getDepartments = departments

    /**
     * Update address field.
     *
     * @param departments New departments field.
     */
    def setDepartments(departments: Seq[String]) {
        this.departments = departments
    }

    /**
     * `toString` implementation.
     */
    override def toString: String =
        "Employee [name=" + name + ", salary=" + salary + ", addr=" + addr + ", departments=" + departments + "]"
}

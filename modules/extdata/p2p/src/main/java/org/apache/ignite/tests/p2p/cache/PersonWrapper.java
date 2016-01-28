/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.tests.p2p.cache;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Wraps Person class.
 */
public class PersonWrapper {
    public static class Person implements Serializable {
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
    }
}

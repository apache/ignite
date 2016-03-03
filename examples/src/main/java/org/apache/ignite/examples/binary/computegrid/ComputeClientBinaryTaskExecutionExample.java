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

package org.apache.ignite.examples.binary.computegrid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.model.Address;
import org.apache.ignite.examples.model.Employee;
import org.apache.ignite.binary.BinaryObject;

/**
 * This example demonstrates use of binary objects with task execution.
 * Specifically it shows that binary objects are simple Java POJOs and do not require any special treatment.
 * <p>
 * The example executes map-reduce task that accepts collection of binary objects as an argument.
 * Since these objects are never deserialized on remote nodes, classes are not required on classpath
 * of these nodes.
 * <p>
 * Remote nodes should always be started with the following command:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link org.apache.ignite.examples.ExampleNodeStartup} in another JVM which will
 * start a node with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeClientBinaryTaskExecutionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Binary objects task execution example started.");

            if (ignite.cluster().forRemotes().nodes().isEmpty()) {
                System.out.println();
                System.out.println(">>> This example requires remote nodes to be started.");
                System.out.println(">>> Please start at least 1 remote node.");
                System.out.println(">>> Refer to example's javadoc for details on configuration.");
                System.out.println();

                return;
            }

            // Generate employees to calculate average salary for.
            Collection<Employee> employees = employees();

            System.out.println();
            System.out.println(">>> Calculating average salary for employees:");

            for (Employee employee : employees)
                System.out.println(">>>     " + employee);

            // Convert collection of employees to collection of binary objects.
            // This allows to send objects across nodes without requiring to have
            // Employee class on classpath of these nodes.
            Collection<BinaryObject> binaries = ignite.binary().toBinary(employees);

            // Execute task and get average salary.
            Long avgSalary = ignite.compute(ignite.cluster().forRemotes()).execute(new ComputeClientTask(), binaries);

            System.out.println();
            System.out.println(">>> Average salary for all employees: " + avgSalary);
            System.out.println();
        }
    }

    /**
     * Creates collection of employees.
     *
     * @return Collection of employees.
     */
    private static Collection<Employee> employees() {
        Collection<Employee> employees = new ArrayList<>();

        employees.add(new Employee(
            "James Wilson",
            12500,
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            Arrays.asList("Human Resources", "Customer Service")
        ));

        employees.add(new Employee(
            "Daniel Adams",
            11000,
            new Address("184 Fidler Drive, San Antonio, TX", 78205),
            Arrays.asList("Development", "QA")
        ));

        employees.add(new Employee(
            "Cristian Moss",
            12500,
            new Address("667 Jerry Dove Drive, Florence, SC", 29501),
            Arrays.asList("Logistics")
        ));

        employees.add(new Employee(
            "Allison Mathis",
            25300,
            new Address("2702 Freedom Lane, Hornitos, CA", 95325),
            Arrays.asList("Development")
        ));

        employees.add(new Employee(
            "Breana Robbin",
            6500,
            new Address("3960 Sundown Lane, Austin, TX", 78758),
            Arrays.asList("Sales")
        ));

        employees.add(new Employee(
            "Philip Horsley",
            19800,
            new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
            Arrays.asList("Sales")
        ));

        employees.add(new Employee(
            "Brian Peters",
            10600,
            new Address("1407 Pearlman Avenue, Boston, MA", 12110),
            Arrays.asList("Development", "QA")
        ));

        employees.add(new Employee(
            "Jack Yang",
            12900,
            new Address("4425 Parrish Avenue Smithsons Valley, TX", 78130),
            Arrays.asList("Sales")
        ));

        return employees;
    }
}

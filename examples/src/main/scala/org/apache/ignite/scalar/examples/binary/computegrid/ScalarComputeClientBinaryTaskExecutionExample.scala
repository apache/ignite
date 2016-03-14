/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.binary.computegrid

import java.util.{ArrayList => JavaArrayList, Arrays => JavaArrays, Collection => JavaCollection}

import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.examples.binary.computegrid.ComputeClientTask
import org.apache.ignite.examples.model.{Address, Employee}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

object ScalarComputeClientBinaryTaskExecutionExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println
        println(">>> Binary objects task execution example started.")

        if (cluster$.forRemotes.nodes.isEmpty) {
            println
            println(">>> This example requires remote nodes to be started.")
            println(">>> Please start at least 1 remote node.")
            println(">>> Refer to example's javadoc for details on configuration.")
            println
        }
        else {
            // Generate employees to calculate average salary for.
            val employees: JavaCollection[Employee] = createEmployees()

            println
            println(">>> Calculating average salary for employees:")

            employees.foreach(employee => println(">>>     " + employee))

            // Convert collection of employees to collection of binary objects.
            // This allows to send objects across nodes without requiring to have
            // Employee class on classpath of these nodes.
            val binaries: JavaCollection[BinaryObject] = ignite$.binary.toBinary(employees)

            // Execute task and get average salary.
            val avgSalary = ignite$.compute(cluster$.forRemotes).execute(new ComputeClientTask, binaries)

            println
            println(">>> Average salary for all employees: " + avgSalary)
            println
        }
    }

    /**
      * Creates collection of employees.
      *
      * @return Collection of employees.
      */
    private def createEmployees(): JavaCollection[Employee] = {
        val employees = new JavaArrayList[Employee]
        employees.add(new Employee("James Wilson", 12500, new Address("1096 Eddy Street, San Francisco, CA", 94109),
            JavaArrays.asList("Human Resources", "Customer Service")))
        employees.add(new Employee("Daniel Adams", 11000, new Address("184 Fidler Drive, San Antonio, TX", 78205),
            JavaArrays.asList("Development", "QA")))
        employees.add(new Employee("Cristian Moss", 12500, new Address("667 Jerry Dove Drive, Florence, SC", 29501),
            JavaArrays.asList("Logistics")))
        employees.add(new Employee("Allison Mathis", 25300, new Address("2702 Freedom Lane, Hornitos, CA", 95325),
            JavaArrays.asList("Development")))
        employees.add(new Employee("Breana Robbin", 6500, new Address("3960 Sundown Lane, Austin, TX", 78758),
            JavaArrays.asList("Sales")))
        employees.add(new Employee("Philip Horsley", 19800, new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
            JavaArrays.asList("Sales")))
        employees.add(new Employee("Brian Peters", 10600, new Address("1407 Pearlman Avenue, Boston, MA", 12110),
            JavaArrays.asList("Development", "QA")))
        employees.add(new Employee("Jack Yang", 12900, new Address("4425 Parrish Avenue Smithsons Valley, TX", 78130),
            JavaArrays.asList("Sales")))

        employees
    }
}

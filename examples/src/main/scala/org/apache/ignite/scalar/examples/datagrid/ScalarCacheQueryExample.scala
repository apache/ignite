/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheMode._
import org.apache.ignite.cache.affinity.AffinityKey
import org.apache.ignite.cache.query._
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import javax.cache.Cache
import java.lang.{Iterable => JavaIterable}
import java.util.UUID

import scala.collection.JavaConversions._

/**
 * Demonstrates cache ad-hoc queries with Scalar.
 * <p/>
 * Remote nodes should be started using `ExampleNodeStartup` which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheQueryExample {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Organizations cache name. */
    private val ORG_CACHE = ScalarCacheQueryExample.getClass.getSimpleName + "Organizations"

    /** Persons cache name. */
    private val PERSON_CACHE = ScalarCacheQueryExample.getClass.getSimpleName + "Persons"

    /**
     * Example entry point. No arguments required.
     *
     * @param args Command line arguments. None required.
     */
    def main(args: Array[String]) {
        scalar(CONFIG) {
            val orgCache = createCache$(ORG_CACHE, PARTITIONED, Seq(classOf[UUID], classOf[Organization]))

            try {
                val personCache = createCache$(PERSON_CACHE, PARTITIONED, Seq(classOf[AffinityKey[_]], classOf[Person]))

                try {
                    initialize()

                    scanQuery()

                    sqlQuery()

                    sqlQueryWithJoin()

                    textQuery()

                    sqlQueryWithAggregation()

                    sqlFieldsQuery()

                    sqlFieldsQueryWithJoin()

                    print("Cache query example finished.")
                }
                finally {
                    if (personCache != null)
                        personCache.close()
                }
            }
            finally {
                if (orgCache != null)
                    orgCache.close()
            }
        }
    }

    /**
     * Gets instance of typed cache view to use.
     *
     * @return Cache to use.
     */
    private def mkCache[K, V](cacheName: String): IgniteCache[K, V] = cache$[K, V](cacheName).get

    /**
     * Example for scan query based on a predicate.
     */
    private def scanQuery() {
        val cache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val scan = new ScanQuery[AffinityKey[UUID], Person](
            new IgniteBiPredicate[AffinityKey[UUID], Person] {
                @impl def apply(key: AffinityKey[UUID], person: Person): Boolean = {
                    person.salary <= 1000
                }
        })

        print("People with salaries between 0 and 1000 (queried with SCAN query): ", cache.query(scan).getAll)
    }

    /**
     * Example for SQL queries based on salary ranges.
     */
    private def sqlQuery() {
        val cache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val sql = "salary > ? and salary <= ?"

        print("People with salaries between 0 and 1000 (queried with SQL query): ",
            cache.query(new SqlQuery[AffinityKey[UUID], Person](classOf[Person], sql)
                .setArgs(0.asInstanceOf[Object], 1000.asInstanceOf[Object])).getAll)

        print("People with salaries between 1000 and 2000 (queried with SQL query): ",
            cache.query(new SqlQuery[AffinityKey[UUID], Person](classOf[Person], sql)
                .setArgs(1000.asInstanceOf[Object], 2000.asInstanceOf[Object])).getAll)
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     */
    private def sqlQueryWithJoin() {
        val cache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val joinSql = "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id " +
            "and lower(org.name) = lower(?)"

        print("Following people are 'ApacheIgnite' employees: ",
            cache.query(new SqlQuery[AffinityKey[UUID], Person](classOf[Person], joinSql).setArgs("ApacheIgnite")).getAll)
        print("Following people are 'Other' employees: ",
            cache.query(new SqlQuery[AffinityKey[UUID], Person](classOf[Person], joinSql).setArgs("Other")).getAll)
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private def textQuery() {
        val cache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val masters: QueryCursor[Cache.Entry[AffinityKey[UUID], Person]] =
            cache.query(new TextQuery[AffinityKey[UUID], Person](classOf[Person], "Master"))

        val bachelors: QueryCursor[Cache.Entry[AffinityKey[UUID], Person]] =
            cache.query(new TextQuery[AffinityKey[UUID], Person](classOf[Person], "Bachelor"))

        print("Following people have 'Master Degree' in their resumes: ", masters.getAll)

        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.getAll)
    }

    /**
     * Example for SQL queries to calculate average salary for a specific organization.
     */
    private def sqlQueryWithAggregation() {
        val cache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val sql = "select avg(salary) " +
            "from Person, \"" + ORG_CACHE + "\".Organization as org " +
            "where Person.orgId = org.id " +
            "and lower(org.name) = lower(?)"

        val cursor = cache.query(new SqlFieldsQuery(sql).setArgs("ApacheIgnite"))

        print("Average salary for 'ApacheIgnite' employees: ", cursor.getAll)
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private def sqlFieldsQuery() {
        val cache = ignite$.cache[AffinityKey[UUID], Person](PERSON_CACHE)

        // Execute query to get names of all employees.
        val cursor = cache.query(new SqlFieldsQuery("select concat(firstName, ' ', lastName) from Person"))

        val res = cursor.getAll

        print("Names of all employees:", res)
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private def sqlFieldsQueryWithJoin() {
        val cache = ignite$.cache[AffinityKey[UUID], Person](PERSON_CACHE)

        val sql = "select concat(firstName, ' ', lastName), org.name " + "from Person, \"" +
            ORG_CACHE + "\".Organization as org " + "where Person.orgId = org.id"

        val cursor = cache.query(new SqlFieldsQuery(sql))

        val res = cursor.getAll

        print("Names of all employees and organizations they belong to:", res)
    }

    /**
     * Populate cache with test data.
     */
    private def initialize() {
        val orgCache = mkCache[UUID, Organization](ORG_CACHE)

        val org1 = new Organization("ApacheIgnite")
        val org2 = new Organization("Other")

        orgCache += (org1.id -> org1)
        orgCache += (org2.id -> org2)

        val personCache = mkCache[AffinityKey[UUID], Person](PERSON_CACHE)

        val p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.")
        val p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.")
        val p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.")
        val p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.")

        personCache += (p1.key -> p1)
        personCache += (p2.key -> p2)
        personCache += (p3.key -> p3)
        personCache += (p4.key -> p4)
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private def print(msg: String) {
        assert(msg != null)

        println()
        println(">>> " + msg)
    }

    /**
     * Prints object or collection of objects to standard out.
     *
     * @param msg Message to print before object is printed.
     * @param res Collection of result object to print.
     */
    private def print(msg: String, res: JavaIterable[_]) {
        assert(msg != null)
        assert(res != null)

        print(msg)

        res.foreach(e => println(">>>     " + e.toString))
    }

    /**
     * Organization class.
     */
    private case class Organization(
        @ScalarCacheQuerySqlField
        name: String) {
        /** Organization ID. */
        @ScalarCacheQuerySqlField
        val id = UUID.randomUUID
    }

    /**
     * Person class.
     */
    private case class Person(org: Organization,
        @ScalarCacheQuerySqlField firstName: String,
        @ScalarCacheQuerySqlField lastName: String,
        @ScalarCacheQuerySqlField salary: Double,
        @ScalarCacheQueryTextField resume: String) {
        /** Person ID. */
        val id = UUID.randomUUID

        /** Organization ID. */
        @ScalarCacheQuerySqlField
        val orgId = org.id

        /** Affinity key for this person. */
        val key = new AffinityKey[UUID](id, org.id)

        /**
         * `toString` implementation.
         */
        override def toString: String = {
            firstName + " " + lastName + " [salary: " + salary + ", resume: " + resume + "]"
        }
    }
}

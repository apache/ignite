/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.binary.datagrid

import java.lang.{Integer => JavaInteger}
import java.sql.Timestamp
import java.util.{Arrays => JavaArrays, LinkedHashMap => JavaLinkedHashMap, List => JavaList}
import javax.cache.Cache

import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache._
import org.apache.ignite.cache.query.{QueryCursor, SqlFieldsQuery, SqlQuery, TextQuery}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.model._
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

object ScalarCacheClientBinaryQueryExample extends App {
    /** Organization cache name. */
    private val ORGANIZATION_CACHE_NAME = ScalarCacheClientBinaryQueryExample.getClass.getSimpleName + "Organizations"

    /** Employee cache name. */
    private val EMPLOYEE_CACHE_NAME = ScalarCacheClientBinaryQueryExample.getClass.getSimpleName + "Employees"

    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println
        println(">>> Binary objects cache query example started.")

        val orgCacheCfg = new CacheConfiguration[Integer, Organization]

        orgCacheCfg.setCacheMode(CacheMode.PARTITIONED)
        orgCacheCfg.setName(ORGANIZATION_CACHE_NAME)

        orgCacheCfg.setQueryEntities(JavaArrays.asList(createOrganizationQueryEntity()))

        val employeeCacheCfg = new CacheConfiguration[EmployeeKey, Employee]

        employeeCacheCfg.setCacheMode(CacheMode.PARTITIONED)
        employeeCacheCfg.setName(EMPLOYEE_CACHE_NAME)

        employeeCacheCfg.setQueryEntities(JavaArrays.asList(createEmployeeQueryEntity()))

        val orgCache = createCache$(orgCacheCfg)
        val employeeCache = createCache$(employeeCacheCfg)

        try {
            if (cluster$.forDataNodes(orgCache.getName).nodes.isEmpty) {
                println
                println(">>> This example requires remote cache nodes to be started.")
                println(">>> Please start at least 1 remote cache node.")
                println(">>> Refer to example's javadoc for details on configuration.")
                println
            }
            else {
                populateCache(orgCache, employeeCache)

                val binaryCache: IgniteCache[BinaryObject, BinaryObject] = employeeCache.withKeepBinary()

                sqlQuery(binaryCache)

                sqlJoinQuery(binaryCache)

                sqlFieldsQuery(binaryCache)

                textQuery(binaryCache)

                println
            }
        } finally {
            ignite$.destroyCache(ORGANIZATION_CACHE_NAME)
            ignite$.destroyCache(EMPLOYEE_CACHE_NAME)

            if (orgCache != null)
                orgCache.close()

            if (employeeCache != null)
                employeeCache.close()
        }
    }

    /**
      * Create cache type metadata for [[Employee]].
      *
      * @return Cache type metadata.
      */
    private def createEmployeeQueryEntity(): QueryEntity = {
        val employeeEntity = new QueryEntity

        employeeEntity.setValueType(classOf[Employee].getName)
        employeeEntity.setKeyType(classOf[EmployeeKey].getName)

        val fields = new JavaLinkedHashMap[String, String]

        fields.put("name", classOf[String].getName)
        fields.put("salary", classOf[Long].getName)
        fields.put("addr.zip", classOf[Integer].getName)
        fields.put("organizationId", classOf[Integer].getName)
        fields.put("addr.street", classOf[Integer].getName)

        employeeEntity.setFields(fields)

        employeeEntity.setIndexes(JavaArrays.asList(new QueryIndex("name"), new QueryIndex("salary"),
            new QueryIndex("zip"), new QueryIndex("organizationId"), new QueryIndex("street", QueryIndexType.FULLTEXT)))

        employeeEntity
    }

    /**
      * Create cache type metadata for [[Organization]].
      *
      * @return Cache type metadata.
      */
    private def createOrganizationQueryEntity(): QueryEntity = {
        val organizationEntity = new QueryEntity

        organizationEntity.setValueType(classOf[Organization].getName)
        organizationEntity.setKeyType(classOf[Integer].getName)

        val fields = new JavaLinkedHashMap[String, String]

        fields.put("name", classOf[String].getName)
        fields.put("address.street", classOf[String].getName)

        organizationEntity.setFields(fields)

        organizationEntity.setIndexes(JavaArrays.asList(new QueryIndex("name")))

        organizationEntity
    }

    /**
      * Queries employees that have provided ZIP code in address.
      *
      * @param cache Ignite cache.
      */
    private def sqlQuery(cache: IgniteCache[BinaryObject, BinaryObject]) {
        val query = new SqlQuery[BinaryObject, BinaryObject](classOf[Employee], "zip = ?")

        val zip = new JavaInteger(94109)

        val employees: QueryCursor[Cache.Entry[BinaryObject, BinaryObject]] = cache.query(query.setArgs(zip))

        println
        println(">>> Employees with zip " + zip + ':')

        employees.getAll.foreach(e => println(">>>     " + e.getValue.deserialize))
    }

    /**
      * Queries employees that work for organization with provided name.
      *
      * @param cache Ignite cache.
      */
    private def sqlJoinQuery(cache: IgniteCache[BinaryObject, BinaryObject]) {
        val qry = new SqlQuery[BinaryObject, BinaryObject](classOf[Employee],
            "from Employee, \"" + ORGANIZATION_CACHE_NAME + "\".Organization as org " +
            "where Employee.organizationId = org._key and org.name = ?")

        val organizationName = "GridGain"

        val employees: QueryCursor[Cache.Entry[BinaryObject, BinaryObject]] = cache.query(qry.setArgs(organizationName))

        println
        println(">>> Employees working for " + organizationName + ':')

        employees.getAll.foreach(e => println(">>>     " + e.getValue))
    }

    /**
      * Queries names and salaries for all employees.
      *
      * @param cache Ignite cache.
      */
    private def sqlFieldsQuery(cache: IgniteCache[BinaryObject, BinaryObject]) {
        val qry = new SqlFieldsQuery("select name, salary from Employee")

        val employees: QueryCursor[JavaList[_]] = cache.query(qry)

        println
        println(">>> Employee names and their salaries:")

        employees.getAll.foreach(row => println(">>>     [Name=" + row.get(0) + ", salary=" + row.get(1) + ']'))
    }

    /**
      * Queries employees that live in Texas using full-text query API.
      *
      * @param cache Ignite cache.
      */
    private def textQuery(cache: IgniteCache[BinaryObject, BinaryObject]) {
        val qry = new TextQuery[BinaryObject, BinaryObject](classOf[Employee], "TX")

        val employees: QueryCursor[Cache.Entry[BinaryObject, BinaryObject]] = cache.query(qry)

        println
        println(">>> Employees living in Texas:")

        employees.getAll.foreach(e => println(">>>     " + e.getValue.deserialize))
    }

    /**
      * Populates cache with data.
      *
      * @param orgCache Organization cache.
      * @param employeeCache Employee cache.
      */
    @SuppressWarnings(Array("TypeMayBeWeakened")) private def populateCache(orgCache: IgniteCache[Integer, Organization],
        employeeCache: IgniteCache[EmployeeKey, Employee]) {
        orgCache.put(1, new Organization(
            "GridGain",
            new Address("1065 East Hillsdale Blvd, Foster City, CA", 94404),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis))
        )

        orgCache.put(2, new Organization(
            "Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis))
        )

        employeeCache.put(new EmployeeKey(1, 1),
            new Employee("James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                JavaArrays.asList("Human Resources", "Customer Service"))
        )

        employeeCache.put(new EmployeeKey(2, 1),
            new Employee("Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                JavaArrays.asList("Development", "QA"))
        )

        employeeCache.put(new EmployeeKey(3, 1),
            new Employee("Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                JavaArrays.asList("Logistics"))
        )

        employeeCache.put(new EmployeeKey(4, 2),
            new Employee("Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                JavaArrays.asList("Development"))
        )

        employeeCache.put(new EmployeeKey(5, 2),
            new Employee("Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                JavaArrays.asList("Sales"))
        )

        employeeCache.put(new EmployeeKey(6, 2),
            new Employee("Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                JavaArrays.asList("Sales"))
        )

        employeeCache.put(new EmployeeKey(7, 2),
            new Employee("Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                JavaArrays.asList("Development", "QA"))
        )
    }
}

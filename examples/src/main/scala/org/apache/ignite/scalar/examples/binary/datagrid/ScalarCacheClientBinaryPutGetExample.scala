/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.binary.datagrid

import java.sql.Timestamp
import java.util.{ArrayList => JavaArrayList, HashMap => JavaHashMap}

import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.{CacheAtomicityMode, CacheMode}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.model.{Address, Organization, OrganizationType}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

object ScalarCacheClientBinaryPutGetExample extends App {
    /** Cache name. */
    private val CACHE_NAME = ScalarCacheClientBinaryPutGetExample.getClass.getSimpleName

    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println(">>> Binary objects cache put-get example started.")

        if (cluster$.forDataNodes(CACHE_NAME).nodes.isEmpty) {
            println
            println(">>> This example requires remote cache node nodes to be started.")
            println(">>> Please start at least 1 remote cache node.")
            println(">>> Refer to example's javadoc for details on configuration.")
            println
        }
        else {
            val cfg = new CacheConfiguration[Integer, Organization]

            cfg.setCacheMode(CacheMode.PARTITIONED)
            cfg.setName(CACHE_NAME)
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC)

            val cache = createCache$(cfg)

            try {
                putGet(cache)
                putGetBinary(cache)
                putGetAll(cache)
                putGetAllBinary(cache)
                println
            } finally {
                ignite$.destroyCache(CACHE_NAME)

                if (cache != null)
                    cache.close()
            }
        }
    }

    /**
      * Execute individual put and get.
      *
      * @param cache Cache.
      */
    private def putGet(cache: IgniteCache[Integer, Organization]) {
        val org = new Organization("Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis)
        )

        cache.put(1, org)

        val orgFromCache = cache.get(1)

        println
        println(">>> Retrieved organization instance from cache: " + orgFromCache)
    }

    /**
      * Execute individual put and get, getting value in binary format, without de-serializing it.
      *
      * @param cache Cache.
      */
    private def putGetBinary(cache: IgniteCache[Integer, Organization]) {
        val org = new Organization("Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis)
        )

        cache.put(1, org)

        val binaryCache: IgniteCache[Integer, BinaryObject] = cache.withKeepBinary()

        val po = binaryCache.get(1)

        val name: String = po.field("name")

        println
        println(">>> Retrieved organization name from binary object: " + name)
    }

    /**
      * Execute bulk `putAll(...)` and `getAll(...)` operations.
      *
      * @param cache Cache.
      */
    private def putGetAll(cache: IgniteCache[Integer, Organization]) {
        val org1 = new Organization("Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis)
        )

        val org2 = new Organization("Red Cross",
            new Address("184 Fidler Drive, San Antonio, TX", 78205),
            OrganizationType.NON_PROFIT,
            new Timestamp(System.currentTimeMillis)
        )

        val map = new JavaHashMap[Integer, Organization]

        map.put(1, org1)
        map.put(2, org2)

        cache.putAll(map)

        val mapFromCache = cache.getAll(map.keySet)

        println
        println(">>> Retrieved organization instances from cache:")

        mapFromCache.values.foreach(org => println(">>>     " + org))
    }

    /**
      * Execute bulk `putAll(...)` and `getAll(...)` operations,
      * getting values in binary format, without de-serializing it.
      *
      * @param cache Cache.
      */
    private def putGetAllBinary(cache: IgniteCache[Integer, Organization]) {
        val org1 = new Organization("Microsoft",
            new Address("1096 Eddy Street, San Francisco, CA", 94109),
            OrganizationType.PRIVATE,
            new Timestamp(System.currentTimeMillis)
        )

        val org2 = new Organization("Red Cross",
            new Address("184 Fidler Drive, San Antonio, TX", 78205),
            OrganizationType.NON_PROFIT,
            new Timestamp(System.currentTimeMillis)
        )

        val map = new JavaHashMap[Integer, Organization]

        map.put(1, org1)
        map.put(2, org2)

        cache.putAll(map)

        val binaryCache: IgniteCache[Integer, BinaryObject] = cache.withKeepBinary()
        val poMap = binaryCache.getAll(map.keySet)
        val names = new JavaArrayList[String]

        poMap.values.foreach(po => names.add(po.field[String]("name")))

        println
        println(">>> Retrieved organization names from binary objects: " + names)
    }
}

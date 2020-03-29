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

package org.apache.ignite.scalar.examples

import java.lang.{Integer => JavaInt}
import java.util.ConcurrentModificationException
import java.util.concurrent.ThreadLocalRandom
import javax.cache.Cache

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

/**
 * <a href="http://en.wikipedia.org/wiki/Snowflake_schema">Snowflake Schema</a> is a logical
 * arrangement of data in which data is split into `dimensions`  and `facts`
 * <i>Dimensions</i> can be referenced or joined by other <i>dimensions</i> or <i>facts</i>,
 * however, <i>facts</i> are generally not referenced by other facts. You can view <i>dimensions</i>
 * as your master or reference data, while <i>facts</i> are usually large data sets of events or
 * other objects that continuously come into the system and may change frequently. In Ignite
 * such architecture is supported via cross-cache queries. By storing <i>dimensions</i> in
 * `CacheMode#REPLICATED REPLICATED` caches and <i>facts</i> in much larger
 * `CacheMode#PARTITIONED PARTITIONED` caches you can freely execute distributed joins across
 * your whole in-memory data ignite cluster, thus querying your in memory data without any limitations.
 * <p/>
 * In this example we have two <i>dimensions</i>, `DimProduct` and `DimStore` and
 * one <i>fact</i> - `FactPurchase`. Queries are executed by joining dimensions and facts
 * in various ways.
 * <p/>
 * Remote nodes should be started using `ExampleNodeStartup` which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarSnowflakeSchemaExample {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Name of partitioned cache specified in spring configuration. */
    private val PARTITIONED_CACHE_NAME = "ScalarSnowflakeSchemaExamplePartitioned"

    /** Name of replicated cache specified in spring configuration. */
    private val REPLICATED_CACHE_NAME = "ScalarSnowflakeSchemaExampleReplicated"

    /** ID generator. */
    private[this] val idGen = Stream.from(0).iterator

    /** DimStore data. */
    private[this] val dataStore = scala.collection.mutable.Map[JavaInt, DimStore]()

    /** DimProduct data. */
    private[this] val dataProduct = scala.collection.mutable.Map[JavaInt, DimProduct]()

    /**
     * Example entry point. No arguments required.
     */
    def main(args: Array[String]) {
        scalar(CONFIG) {
            println
            println(">>> Cache star schema example started.")

            // Destroy caches to clean up the data if any left from previous runs.
            destroyCache$(PARTITIONED_CACHE_NAME)
            destroyCache$(REPLICATED_CACHE_NAME)

            val dimCache = createCache$[JavaInt, AnyRef](REPLICATED_CACHE_NAME, CacheMode.REPLICATED, Seq(classOf[JavaInt], classOf[DimStore],
                classOf[JavaInt], classOf[DimProduct]))

            try {
                val factCache = createCache$[JavaInt, FactPurchase](PARTITIONED_CACHE_NAME, indexedTypes = Seq(classOf[JavaInt], classOf[FactPurchase]))

                try {
                    populateDimensions(dimCache)
                    populateFacts(factCache)

                    queryStorePurchases()
                    queryProductPurchases()
                }
                finally {
                    factCache.destroy()
                }
            }
            finally {
                dimCache.destroy()
            }
        }
    }

    /**
     * Populate cache with `dimensions` which in our case are
     * `DimStore` and `DimProduct` instances.
     */
    def populateDimensions(dimCache: IgniteCache[JavaInt, AnyRef]) {
        val store1 = new DimStore(idGen.next(), "Store1", "12345", "321 Chilly Dr, NY")
        val store2 = new DimStore(idGen.next(), "Store2", "54321", "123 Windy Dr, San Francisco")

        // Populate stores.
        dimCache.put(store1.id, store1)
        dimCache.put(store2.id, store2)

        dataStore.put(store1.id, store1)
        dataStore.put(store2.id, store2)

        for (i <- 1 to 20) {
            val product = new DimProduct(idGen.next(), "Product" + i, i + 1, (i + 1) * 10)

            dimCache.put(product.id, product)

            dataProduct.put(product.id, product)
        }
    }

    /**
     * Populate cache with `facts`, which in our case are `FactPurchase` objects.
     */
    def populateFacts(factCache: IgniteCache[JavaInt, FactPurchase]) {
        for (i <- 1 to 100) {
            val store: DimStore = rand(dataStore.values)
            val prod: DimProduct = rand(dataProduct.values)
            val purchase: FactPurchase = new FactPurchase(idGen.next(), prod.id, store.id, i + 1)

            factCache.put(purchase.id, purchase)
        }
    }

    /**
     * Query all purchases made at a specific store. This query uses cross-cache joins
     * between `DimStore` objects stored in `replicated` cache and
     * `FactPurchase` objects stored in `partitioned` cache.
     */
    def queryStorePurchases() {
        val factCache = ignite$.cache[JavaInt, FactPurchase](PARTITIONED_CACHE_NAME)

        val storePurchases = factCache.sql(
            "from \"" + REPLICATED_CACHE_NAME + "\".DimStore, \"" + PARTITIONED_CACHE_NAME + "\".FactPurchase " +
            "where DimStore.id=FactPurchase.storeId and DimStore.name=?", "Store1")

        printQueryResults("All purchases made at store1:", storePurchases.getAll)
    }

    /**
     * Query all purchases made at a specific store for 3 specific products.
     * This query uses cross-cache joins between `DimStore`, `DimProduct`
     * objects stored in `replicated` cache and `FactPurchase` objects
     * stored in `partitioned` cache.
     */
    private def queryProductPurchases() {
        val factCache = ignite$.cache[JavaInt, FactPurchase](PARTITIONED_CACHE_NAME)

        // All purchases for certain product made at store2.
        // =================================================
        val p1: DimProduct = rand(dataProduct.values)
        val p2: DimProduct = rand(dataProduct.values)
        val p3: DimProduct = rand(dataProduct.values)

        println("IDs of products [p1=" + p1.id + ", p2=" + p2.id + ", p3=" + p3.id + ']')

        val prodPurchases = factCache.sql(
            "from \"" + REPLICATED_CACHE_NAME + "\".DimStore, \"" + REPLICATED_CACHE_NAME + "\".DimProduct, \"" +
                PARTITIONED_CACHE_NAME + "\".FactPurchase " +
            "where DimStore.id=FactPurchase.storeId and " +
                "DimProduct.id=FactPurchase.productId and " +
                "DimStore.name=? and DimProduct.id in(?, ?, ?)",
            "Store2", p1.id, p2.id, p3.id)

        printQueryResults("All purchases made at store2 for 3 specific products:", prodPurchases.getAll)
    }

    /**
     * Print query results.
     *
     * @param msg Initial message.
     * @param res Results to print.
     */
    private def printQueryResults[V](msg: String, res: Iterable[Cache.Entry[JavaInt, V]]) {
        println(msg)

        for (e <- res)
            println("    " + e.getValue.toString)
    }

    /**
     * Gets random value from given collection.
     *
     * @param c Input collection (no `null` and not emtpy).
     * @return Random value from the input collection.
     */
    def rand[T](c: Iterable[_ <: T]): T = {
        val n: Int = ThreadLocalRandom.current.nextInt(c.size)

        var i: Int = 0

        for (t <- c) {
            if (i < n)
                i += 1
            else
                return t
        }

        throw new ConcurrentModificationException
    }
}

/**
 * Represents a physical store location. In our `snowflake` schema a `store`
 * is a `dimension` and will be cached in `CacheMode#REPLICATED` cache.
 *
 * @param id Primary key.
 * @param name Store name.
 * @param zip Zip code.
 * @param addr Address.
 */
class DimStore(
    @ScalarCacheQuerySqlField
    val id: Int,
    @ScalarCacheQuerySqlField
    val name: String,
    val zip: String,
    val addr: String) {
    /**
     * `toString` implementation.
     */
    override def toString: String = {
        val sb: StringBuilder = new StringBuilder

        sb.append("DimStore ")
        sb.append("[id=").append(id)
        sb.append(", name=").append(name)
        sb.append(", zip=").append(zip)
        sb.append(", addr=").append(addr)
        sb.append(']')

        sb.toString()
    }
}

/**
 * Represents a product available for purchase. In our `snowflake` schema a `product`
 * is a `dimension` and will be cached in `CacheMode#REPLICATED` cache.
 *
 * @param id Product ID.
 * @param name Product name.
 * @param price Product list price.
 * @param qty Available product quantity.
 */
class DimProduct(
    @ScalarCacheQuerySqlField
    val id: Int,
    val name: String,
    @ScalarCacheQuerySqlField
    val price: Float,
    val qty: Int) {
    /**
     * `toString` implementation.
     */
    override def toString: String = {
        val sb: StringBuilder = new StringBuilder

        sb.append("DimProduct ")
        sb.append("[id=").append(id)
        sb.append(", name=").append(name)
        sb.append(", price=").append(price)
        sb.append(", qty=").append(qty)
        sb.append(']')

        sb.toString()
    }
}

/**
 * Represents a purchase record. In our `snowflake` schema purchase
 * is a `fact` and will be cached in larger `CacheMode#PARTITIONED` cache.
 *
 * @param id Purchase ID.
 * @param productId Purchased product ID.
 * @param storeId Store ID.
 * @param purchasePrice Purchase price.
 */
class FactPurchase(
    @ScalarCacheQuerySqlField
    val id: Int,
    @ScalarCacheQuerySqlField
    val productId: Int,
    @ScalarCacheQuerySqlField
    val storeId: Int,
    @ScalarCacheQuerySqlField
    val purchasePrice: Float) {
    /**
     * `toString` implementation.
     */
    override def toString: String = {
        val sb: StringBuilder = new StringBuilder

        sb.append("FactPurchase ")
        sb.append("[id=").append(id)
        sb.append(", productId=").append(productId)
        sb.append(", storeId=").append(storeId)
        sb.append(", purchasePrice=").append(purchasePrice)
        sb.append(']')

        sb.toString()
    }
}

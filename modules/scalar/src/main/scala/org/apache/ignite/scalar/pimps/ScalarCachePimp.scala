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

package org.apache.ignite.scalar.pimps

import org.apache.ignite.cache.query._

import javax.cache.Cache

import org.apache.ignite._
import org.apache.ignite.lang.{IgnitePredicate, IgniteReducer}
import org.apache.ignite.scalar.scalar._
import org.jetbrains.annotations.Nullable

import java.util.{List => JavaList, Set => JavaSet}

import scala.collection._
import scala.collection.JavaConversions._

/**
 * Companion object.
 */
object ScalarCachePimp {
    /**
     * Creates new Scalar cache projection pimp with given Java-side implementation.
     *
     * @param impl Java-side implementation.
     */
    def apply[K, V](impl: IgniteCache[K, V]) = {
        if (impl == null)
            throw new NullPointerException("impl")

        val pimp = new ScalarCachePimp[K, V]

        pimp.impl = impl

        pimp
    }
}

/**
 * ==Overview==
 * Defines Scalar "pimp" for `IgniteCache` on Java side.
 *
 * Essentially this class extends Java `IgniteCache` interface with Scala specific
 * API adapters using primarily implicit conversions defined in `ScalarConversions` object. What
 * it means is that you can use functions defined in this class on object
 * of Java `IgniteCache` type. Scala will automatically (implicitly) convert it into
 * Scalar's pimp and replace the original call with a call on that pimp.
 *
 * Note that Scalar provide extensive library of implicit conversion between Java and
 * Scala Ignite counterparts in `ScalarConversions` object
 *
 * ==Suffix '$' In Names==
 * Symbol `$` is used in names when they conflict with the names in the base Java class
 * that Scala pimp is shadowing or with Java package name that your Scala code is importing.
 * Instead of giving two different names to the same function we've decided to simply mark
 * Scala's side method with `$` suffix.
 */
class ScalarCachePimp[@specialized K, @specialized V] extends PimpedType[IgniteCache[K, V]]
with Iterable[Cache.Entry[K, V]] with Ordered[IgniteCache[K, V]] {
    /** */
    lazy val value: IgniteCache[K, V] = impl

    /** */
    protected var impl: IgniteCache[K, V] = _

    /** Type alias. */
    protected type EntryPred = (Cache.Entry[K, V]) => Boolean

    /** Type alias. */
    protected type KvPred = (K, V) => Boolean

    protected def toJavaSet[T](it: Iterable[T]): JavaSet[T] = new java.util.HashSet[T](asJavaCollection(it))

    /**
     * Compares this cache name to the given cache name.
     *
     * @param that Another cache instance to compare names with.
     */
    def compare(that: IgniteCache[K, V]): Int = that.getName.compareTo(value.getName)

    /**
     * Gets iterator for cache entries.
     */
    def iterator = toScalaSeq(value.iterator).iterator

    /**
     * Unwraps sequence of functions to sequence of Ignite predicates.
     */
    private def unwrap(@Nullable p: Seq[EntryPred]): Seq[IgnitePredicate[Cache.Entry[K, V]]] =
        if (p == null)
            null
        else
            p map ((f: EntryPred) => toPredicate(f))

    /**
     * Converts reduce function to Grid Reducer that takes map entries.
     *
     * @param rdc Reduce function.
     * @return Entry reducer.
     */
    private def toEntryReducer[R](rdc: Iterable[(K, V)] => R): IgniteReducer[java.util.Map.Entry[K, V], R] = {
        new IgniteReducer[java.util.Map.Entry[K, V], R] {
            private var seq = Seq.empty[(K, V)]

            def collect(e: java.util.Map.Entry[K, V]): Boolean = {
                seq +:= (e.getKey, e.getValue)

                true
            }

            def reduce(): R = {
                rdc(seq)
            }
        }
    }

    /**
     * Retrieves value mapped to the specified key from cache. The return value of `null`
     * means entry did not pass the provided filter or cache has no mapping for the key.
     *
     * @param k Key to retrieve the value for.
     * @return Value for the given key.
     */
    def apply(k: K): V =
        value.get(k)

    /**
     * Returns the value associated with a key, or a default value if the key is not contained in the map.
     *
     * @param k The key.
     * @param default A computation that yields a default value in case key is not in cache.
     * @return The cache value associated with `key` if it exists, otherwise the result
     *      of the `default` computation.
     */
    def getOrElse(k: K, default: => V) = {
        opt(k) match {
            case Some(v) => v
            case None => default
        }
    }

    /**
     * Retrieves value mapped to the specified key from cache as an option. The return value
     * of `null` means entry did not pass the provided filter or cache has no mapping for the key.
     *
     * @param k Key to retrieve the value for.
     * @return Value for the given key.
     * @see `IgniteCache.get(...)`
     */
    def opt(k: K): Option[V] =
        Option(value.get(k))

    /**
     * Converts given type of corresponding Java type, if Scala does
     * auto-conversion for a given type. Only primitive types and Strings
     * are supported.
     *
     * @param c Type to convert.
     */
    private def toJavaType(c: Class[_]) = {
        assert(c != null)

        // Hopefully if-else is faster here than a normal matching.
        if (c == classOf[Int])
            classOf[java.lang.Integer]
        else if (c == classOf[Boolean])
            classOf[java.lang.Boolean]
        else if (c == classOf[String])
            classOf[java.lang.String]
        else if (c == classOf[Char])
            classOf[java.lang.Character]
        else if (c == classOf[Long])
            classOf[java.lang.Long]
        else if (c == classOf[Double])
            classOf[java.lang.Double]
        else if (c == classOf[Float])
            classOf[java.lang.Float]
        else if (c == classOf[Short])
            classOf[java.lang.Short]
        else if (c == classOf[Byte])
            classOf[java.lang.Byte]
        else if (c == classOf[Symbol])
            throw new IgniteCheckedException("Cache type projeciton on 'scala.Symbol' are not supported.")
        else
            c
    }

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * If write-through is enabled, the stored value will be persisted to `GridCacheStore`
     * via `GridCacheStore#put(String, GridCacheTx, Object, Object)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param kv Key-Value pair to store in cache.
     * @return `True` if value was stored in cache, `false` otherwise.
     * @see `IgniteCache#putx(...)`
     */
    def putx$(kv: (K, V)): Boolean = value.putIfAbsent(kv._1, kv._2)

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * If write-through is enabled, the stored value will be persisted to `GridCacheStore`
     * via `GridCacheStore#put(String, GridCacheTx, Object, Object)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param kv Key-Value pair to store in cache.
     * @return Previous value associated with specified key, or `null`
     *      if entry did not pass the filter, or if there was no mapping for the key in swap
     *      or in persistent storage.
     * @see `IgniteCache#put(...)`
     */
    def put$(kv: (K, V)): V = value.getAndReplace(kv._1, kv._2)

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * If write-through is enabled, the stored value will be persisted to `GridCacheStore`
     * via `GridCacheStore#put(String, GridCacheTx, Object, Object)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param kv Key-Value pair to store in cache.
     * @return Previous value associated with specified key as an option.
     * @see `IgniteCache#put(...)`
     */
    def putOpt$(kv: (K, V)): Option[V] = Option(value.getAndReplace(kv._1, kv._2))

    /**
     * Operator alias for the same function `putx$`.
     *
     * @param kv Key-Value pair to store in cache.
     * @return `True` if value was stored in cache, `false` otherwise.
     * @see `IgniteCache#putx(...)`
     */
    def +=(kv: (K, V)): Boolean =
        putx$(kv)

    /**
     * Stores given key-value pairs in cache.
     *
     * If write-through is enabled, the stored values will be persisted to `GridCacheStore`
     * via `GridCacheStore#putAll(String, GridCacheTx, Map)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param kv1 Key-value pair to store in cache.
     * @param kv2 Key-value pair to store in cache.
     * @param kvs Optional key-value pairs to store in cache.
     * @see `IgniteCache#putAll(...)`
     */
    def putAll$(kv1: (K, V), kv2: (K, V), @Nullable kvs: (K, V)*) {
        var m = mutable.Map.empty[K, V]

        m += (kv1, kv2)

        if (kvs != null)
            kvs foreach (m += _)

        value.putAll(m)
    }

    /**
     * Stores given key-value pairs from the sequence in cache.
     *
     * If write-through is enabled, the stored values will be persisted to `GridCacheStore`
     * via `GridCacheStore#putAll(String, GridCacheTx, Map)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param kvs Key-value pairs to store in cache. If `null` this function is no-op.
     * @see `IgniteCache#putAll(...)`
     */
    def putAll$(@Nullable kvs: Seq[(K, V)]) {
        if (kvs != null)
            value.putAll(mutable.Map(kvs: _*))
    }

    /**
     * Removes given key mappings from cache.
     *
     * If write-through is enabled, the values will be removed from `GridCacheStore`
     * via `GridCacheStore#removeAll(String, GridCacheTx, Collection)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param ks Sequence of additional keys to remove. If `null` - this function is no-op.
     * @see `IgniteCache#removeAll(...)`
     */
    def removeAll$(@Nullable ks: Seq[K]) {
        if (ks != null)
            value.removeAll(toJavaSet(ks))
    }

    /**
     * Operator alias for the same function `putAll$`.
     *
     * @param kv1 Key-value pair to store in cache.
     * @param kv2 Key-value pair to store in cache.
     * @param kvs Optional key-value pairs to store in cache.
     * @see `IgniteCache#putAll(...)`
     */
    def +=(kv1: (K, V), kv2: (K, V), @Nullable kvs: (K, V)*) {
        putAll$(kv1, kv2, kvs: _*)
    }

    /**
     * Removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. Otherwise, in case of `CacheMode#REPLICATED` caches,
     * the value will be loaded from swap and, if it's not there, and read-through is allowed,
     * from the underlying `GridCacheStore` storage. In case of `CacheMode#PARTITIONED`
     * caches, the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap and read-through is allowed,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, `GridCacheStore#load(String, GridCacheTx, Object)` method will be used.
     *
     * If the returned value is not needed, method `removex$(...)` should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     *
     * If write-through is enabled, the value will be removed from 'GridCacheStore'
     * via `GridCacheStore#remove(String, GridCacheTx, Object)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @return Previous value associated with specified key, or `null`
     *      if there was no value for this key.
     * @see `IgniteCache#remove(...)`
     */
    def remove$(k: K): V = value.getAndRemove(k)

    /**
     * Removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. Otherwise, in case of `CacheMode#REPLICATED` caches,
     * the value will be loaded from swap and, if it's not there, and read-through is allowed,
     * from the underlying `GridCacheStore` storage. In case of `CacheMode#PARTITIONED`
     * caches, the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap and read-through is allowed,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, `GridCacheStore#load(String, GridCacheTx, Object)` method will be used.
     *
     * If the returned value is not needed, method `removex$(...)` should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     *
     * If write-through is enabled, the value will be removed from 'GridCacheStore'
     * via `GridCacheStore#remove(String, GridCacheTx, Object)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @return Previous value associated with specified key as an option.
     * @see `IgniteCache#remove(...)`
     */
    def removeOpt$(k: K): Option[V] =
        Option(value.getAndRemove(k))

    /**
     * Operator alias for the same function `remove$`.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @return Previous value associated with specified key, or `null`
     *      if there was no value for this key.
     * @see `IgniteCache#remove(...)`
     */
    def -=(k: K): V = remove$(k)

    /**
     * Removes given key mappings from cache.
     *
     * If write-through is enabled, the values will be removed from `GridCacheStore`
     * via `GridCacheStore#removeAll(String, GridCacheTx, Collection)` method.
     *
     * ===Transactions===
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param k1 1st key to remove.
     * @param k2 2nd key to remove.
     * @param ks Optional sequence of additional keys to remove.
     * @see `IgniteCache#removeAll(...)`
     */
    def removeAll$(k1: K, k2: K, @Nullable ks: K*) {
        val s = new mutable.ArrayBuffer[K](2 + (if (ks == null) 0 else ks.length))

        s += k1
        s += k2

        if (ks != null)
            ks foreach (s += _)

        value.removeAll(toJavaSet(s))
    }

    /**
     * Operator alias for the same function `remove$`.
     *
     * @param k1 1st key to remove.
     * @param k2 2nd key to remove.
     * @param ks Optional sequence of additional keys to remove.
     * @see `IgniteCache#removeAll(...)`
     */
    def -=(k1: K, k2: K, @Nullable ks: K*) {
        removeAll$(k1, k2, ks: _*)
    }

    /**
     * Creates and executes ad-hoc `SCAN` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `CacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(cls: Class[_ <: V], kvp: KvPred): QueryCursor[Cache.Entry[K, V]] = {
        assert(cls != null)
        assert(kvp != null)

        value.query(new ScanQuery(kvp))
    }

    /**
     * Creates and executes ad-hoc `SCAN` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * Note that query value class will be taken implicitly as exact type `V` of this
     * cache projection.
     *
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `CacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(kvp: KvPred)(implicit m: Manifest[V]): QueryCursor[Cache.Entry[K, V]] = {
        assert(kvp != null)

        scan(m.erasure.asInstanceOf[Class[V]], kvp)
    }

    /**
     * Creates and executes ad-hoc `SQL` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `CacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(cls: Class[_ <: V], clause: String, args: Any*): QueryCursor[Cache.Entry[K, V]] = {
        assert(cls != null)
        assert(clause != null)
        assert(args != null)

        val query = new SqlQuery[K, V](cls, clause)

        if (args != null && args.size > 0)
            query.setArgs(args.map(_.asInstanceOf[AnyRef]) : _*)

        value.query(query)
    }

    /**
     * Creates and executes ad-hoc `SQL` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `CacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def sql(cls: Class[_ <: V], clause: String): QueryCursor[Cache.Entry[K, V]] = {
        assert(cls != null)
        assert(clause != null)

        sql(cls, clause, Nil:_*)
    }

    /**
     * Creates and executes ad-hoc `SQL` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * Note that query value class will be taken implicitly as exact type `V` of this
     * cache projection.
     *
     * @param clause Query SQL clause. See `CacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(clause: String, args: Any*)
        (implicit m: Manifest[V]): QueryCursor[Cache.Entry[K, V]] = {
        assert(clause != null)
        assert(args != null)

        sql(m.erasure.asInstanceOf[Class[V]], clause, args:_*)
    }

    /**
     * Creates and executes ad-hoc `TEXT` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query text clause. See `CacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(cls: Class[_ <: V], clause: String): QueryCursor[Cache.Entry[K, V]] = {
        assert(cls != null)
        assert(clause != null)

        value.query(new TextQuery(cls, clause))
    }

    /**
     * Creates and executes ad-hoc `TEXT` query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * Note that query value class will be taken implicitly as exact type `V` of this
     * cache projection.
     *
     * @param clause Query text clause. See `CacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(clause: String)(implicit m: Manifest[V]): QueryCursor[Cache.Entry[K, V]] = {
        assert(clause != null)

        text(m.erasure.asInstanceOf[Class[V]], clause)
    }

    /**
     * Creates and executes ad-hoc `SQL` fields query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param clause Query SQL clause. See `CacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Sequence of sequences of field values.
     */
    def sqlFields(clause: String, args: Any*): QueryCursor[JavaList[_]] = {
        assert(clause != null)
        assert(args != null)

        val query = new SqlFieldsQuery(clause)

        if (args != null && args.nonEmpty)
            query.setArgs(args.map(_.asInstanceOf[AnyRef]) : _*)

        value.query(query)
    }

    /**
     * Creates and executes ad-hoc `SQL` no-arg fields query returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param clause Query SQL clause. See `CacheQuery` for more details.
     * @return Sequence of sequences of field values.
     */
    def sqlFields(clause: String): QueryCursor[JavaList[_]] = {
        assert(clause != null)

        sqlFields(clause, Nil:_*)
    }
}

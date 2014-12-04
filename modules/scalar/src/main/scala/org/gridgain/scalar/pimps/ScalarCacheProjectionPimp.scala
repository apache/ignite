/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

import org.apache.ignite.cluster.ClusterGroup

import collection._
import collection.JavaConversions._
import org.jetbrains.annotations.Nullable
import org.gridgain.grid._
import org.gridgain.grid.cache._
import org.gridgain.grid.lang._
import org.gridgain.grid.util.lang.{GridFunc => F}
import org.gridgain.grid.util.scala.impl
import org.gridgain.scalar._
import scalar._

/**
 * Companion object.
 */
object ScalarCacheProjectionPimp {
    /**
     * Creates new Scalar cache projection pimp with given Java-side implementation.
     *
     * @param impl Java-side implementation.
     */
    def apply[K, V](impl: GridCacheProjection[K, V]) = {
        if (impl == null)
            throw new NullPointerException("impl")

        val pimp = new ScalarCacheProjectionPimp[K, V]

        pimp.impl = impl

        pimp
    }
}

/**
 * ==Overview==
 * Defines Scalar "pimp" for `GridCacheProjection` on Java side.
 *
 * Essentially this class extends Java `GridCacheProjection` interface with Scala specific
 * API adapters using primarily implicit conversions defined in `ScalarConversions` object. What
 * it means is that you can use functions defined in this class on object
 * of Java `GridCacheProjection` type. Scala will automatically (implicitly) convert it into
 * Scalar's pimp and replace the original call with a call on that pimp.
 *
 * Note that Scalar provide extensive library of implicit conversion between Java and
 * Scala GridGain counterparts in `ScalarConversions` object
 *
 * ==Suffix '$' In Names==
 * Symbol `$` is used in names when they conflict with the names in the base Java class
 * that Scala pimp is shadowing or with Java package name that your Scala code is importing.
 * Instead of giving two different names to the same function we've decided to simply mark
 * Scala's side method with `$` suffix.
 */
class ScalarCacheProjectionPimp[@specialized K, @specialized V] extends PimpedType[GridCacheProjection[K, V]]
    with Iterable[GridCacheEntry[K, V]] {
    /** */
    lazy val value: GridCacheProjection[K, V] = impl

    /** */
    protected var impl: GridCacheProjection[K, V] = _

    /** Type alias. */
    protected type EntryPred = (GridCacheEntry[K, V]) => Boolean

    /** Type alias. */
    protected type KvPred = (K, V) => Boolean

    /**
     * Gets iterator for cache entries.
     */
    def iterator =
        toScalaSeq(value.iterator).iterator

    /**
     * Unwraps sequence of functions to sequence of GridGain predicates.
     */
    private def unwrap(@Nullable p: Seq[EntryPred]): Seq[GridPredicate[GridCacheEntry[K, V]]] =
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
    private def toEntryReducer[R](rdc: Iterable[(K, V)] => R): GridReducer[java.util.Map.Entry[K, V], R] = {
        new GridReducer[java.util.Map.Entry[K, V], R] {
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

    private def toRemoteTransformer[K, V, T](trans: V => T):
        GridClosure[java.util.Map.Entry[K, V], java.util.Map.Entry[K, T]] = {
        new GridClosure[java.util.Map.Entry[K, V], java.util.Map.Entry[K, T]] {
            @impl def apply(e: java.util.Map.Entry[K, V]): java.util.Map.Entry[K, T] = {
                new GridBiTuple[K, T](e.getKey, trans(e.getValue))
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
     * @param p Filter to check prior to getting the value. Note that filter check
     *      together with getting the value is an atomic operation.
     * @return Value for the given key.
     * @see `org.gridgain.grid.cache.GridCacheProjection.get(...)`
     */
    def opt(k: K, p: EntryPred = null): Option[V] =
        Option(value.projection(p).get(k))

    /**
     * Gets cache projection based on given key-value predicate. Whenever makes sense,
     * this predicate will be used to pre-filter cache operations. If
     * operation passed pre-filtering, this filter will be passed through
     * to cache operations as well.
     *
     * @param p Key-value predicate for this projection. If `null`, then the
     *      same projection is returned.
     * @return Projection for given key-value predicate.
     * @see `org.gridgain.grid.cache.GridCacheProjection.projection(...)`
     */
    def viewByKv(@Nullable p: ((K, V) => Boolean)): GridCacheProjection[K, V] =
        if (p == null)
            value
        else
            value.projection(p)

    /**
     * Gets cache projection based on given entry filter. This filter will be simply passed through
     * to all cache operations on this projection. Unlike `viewByKv` function, this filter
     * will '''not''' be used for pre-filtering.
     *
     * @param p Filter to be passed through to all cache operations. If `null`, then the
     *      same projection is returned.  If cache operation receives its own filter, then filters
     *      will be `anded`.
     * @return Projection based on given filter.
     * @see `org.gridgain.grid.cache.GridCacheProjection.projection(...)`
     */
    def viewByEntry(@Nullable p: EntryPred): GridCacheProjection[K, V] =
        if (p == null)
            value
        else
            value.projection(p)

    /**
     * Gets cache projection only for given key and value type. Only `non-null` key-value
     * pairs that have matching key and value pairs will be used in this projection.
     *
     * Note that this method should be used instead of `projection(...)` on Java side as
     * it properly converts types from Scala counterparts to Java ones.
     *
     * ===Cache Flags===
     * The resulting projection will have flag `GridCacheFlag#STRICT` set on it.
     *
     * @param k Key type.
     * @param v Value type.
     * @return Cache projection for given key and value types.
     * @see `org.gridgain.grid.cache.GridCacheProjection.projection(...)`
     */
    def viewByType[A, B](k: Class[A], v: Class[B]): GridCacheProjection[A, B] = {
        assert(k != null && v != null)

        value.projection(toJavaType(k), toJavaType(v)).asInstanceOf[GridCacheProjection[A, B]]
    }

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
            throw new GridException("Cache type projeciton on 'scala.Symbol' are not supported.")
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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param kv Key-Value pair to store in cache.
     * @param p Optional filter to check prior to putting value in cache. Note
     *      that filter check is atomic with put operation.
     * @return `True` if value was stored in cache, `false` otherwise.
     * @see `org.gridgain.grid.cache.GridCacheProjection#putx(...)`
     */
    def putx$(kv: (K, V), @Nullable p: EntryPred*): Boolean =
        value.putx(kv._1, kv._2, unwrap(p): _*)

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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param kv Key-Value pair to store in cache.
     * @param p Optional filter to check prior to putting value in cache. Note
     *      that filter check is atomic with put operation.
     * @return Previous value associated with specified key, or `null`
     *      if entry did not pass the filter, or if there was no mapping for the key in swap
     *      or in persistent storage.
     * @see `org.gridgain.grid.cache.GridCacheProjection#put(...)`
     */
    def put$(kv: (K, V), @Nullable p: EntryPred*): V =
        value.put(kv._1, kv._2, unwrap(p): _*)

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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param kv Key-Value pair to store in cache.
     * @param p Optional filter to check prior to putting value in cache. Note
     *      that filter check is atomic with put operation.
     * @return Previous value associated with specified key as an option.
     * @see `org.gridgain.grid.cache.GridCacheProjection#put(...)`
     */
    def putOpt$(kv: (K, V), @Nullable p: EntryPred*): Option[V] =
        Option(value.put(kv._1, kv._2, unwrap(p): _*))

    /**
     * Operator alias for the same function `putx$`.
     *
     * @param kv Key-Value pair to store in cache.
     * @param p Optional filter to check prior to putting value in cache. Note
     *      that filter check is atomic with put operation.
     * @return `True` if value was stored in cache, `false` otherwise.
     * @see `org.gridgain.grid.cache.GridCacheProjection#putx(...)`
     */
    def +=(kv: (K, V), @Nullable p: EntryPred*): Boolean =
        putx$(kv, p: _*)

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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param kv1 Key-value pair to store in cache.
     * @param kv2 Key-value pair to store in cache.
     * @param kvs Optional key-value pairs to store in cache.
     * @see `org.gridgain.grid.cache.GridCacheProjection#putAll(...)`
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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param kvs Key-value pairs to store in cache. If `null` this function is no-op.
     * @see `org.gridgain.grid.cache.GridCacheProjection#putAll(...)`
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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param ks Sequence of additional keys to remove. If `null` - this function is no-op.
     * @see `org.gridgain.grid.cache.GridCacheProjection#removeAll(...)`
     */
    def removeAll$(@Nullable ks: Seq[K]) {
        if (ks != null)
            value.removeAll(ks)
    }

    /**
     * Operator alias for the same function `putAll$`.
     *
     * @param kv1 Key-value pair to store in cache.
     * @param kv2 Key-value pair to store in cache.
     * @param kvs Optional key-value pairs to store in cache.
     * @see `org.gridgain.grid.cache.GridCacheProjection#putAll(...)`
     */
    def +=(kv1: (K, V), kv2: (K, V), @Nullable kvs: (K, V)*) {
        putAll$(kv1, kv2, kvs: _*)
    }

    /**
     * Removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. Otherwise, in case of `GridCacheMode#REPLICATED` caches,
     * the value will be loaded from swap and, if it's not there, and read-through is allowed,
     * from the underlying `GridCacheStore` storage. In case of `GridCacheMode#PARTITIONED`
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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @param p Optional filters to check prior to removing value form cache. Note
     *      that filter is checked atomically together with remove operation.
     * @return Previous value associated with specified key, or `null`
     *      if there was no value for this key.
     * @see `org.gridgain.grid.cache.GridCacheProjection#remove(...)`
     */
    def remove$(k: K, @Nullable p: EntryPred*): V =
        value.remove(k, unwrap(p): _*)

    /**
     * Removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. Otherwise, in case of `GridCacheMode#REPLICATED` caches,
     * the value will be loaded from swap and, if it's not there, and read-through is allowed,
     * from the underlying `GridCacheStore` storage. In case of `GridCacheMode#PARTITIONED`
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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @param p Optional filters to check prior to removing value form cache. Note
     *      that filter is checked atomically together with remove operation.
     * @return Previous value associated with specified key as an option.
     * @see `org.gridgain.grid.cache.GridCacheProjection#remove(...)`
     */
    def removeOpt$(k: K, @Nullable p: EntryPred*): Option[V] =
        Option(value.remove(k, unwrap(p): _*))

    /**
     * Operator alias for the same function `remove$`.
     *
     * @param k Key whose mapping is to be removed from cache.
     * @param p Optional filters to check prior to removing value form cache. Note
     *      that filter is checked atomically together with remove operation.
     * @return Previous value associated with specified key, or `null`
     *      if there was no value for this key.
     * @see `org.gridgain.grid.cache.GridCacheProjection#remove(...)`
     */
    def -=(k: K, @Nullable p: EntryPred*): V =
        remove$(k, p: _*)

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
     * ===Cache Flags===
     * This method is not available if any of the following flags are set on projection:
     * `GridCacheFlag#LOCAL`, `GridCacheFlag#READ`.
     *
     * @param k1 1st key to remove.
     * @param k2 2nd key to remove.
     * @param ks Optional sequence of additional keys to remove.
     * @see `org.gridgain.grid.cache.GridCacheProjection#removeAll(...)`
     */
    def removeAll$(k1: K, k2: K, @Nullable ks: K*) {
        val s = new mutable.ArrayBuffer[K](2 + (if (ks == null) 0 else ks.length))

        s += k1
        s += k2

        if (ks != null)
            ks foreach (s += _)

        value.removeAll(s)
    }

    /**
     * Operator alias for the same function `remove$`.
     *
     * @param k1 1st key to remove.
     * @param k2 2nd key to remove.
     * @param ks Optional sequence of additional keys to remove.
     * @see `org.gridgain.grid.cache.GridCacheProjection#removeAll(...)`
     */
    def -=(k1: K, k2: K, @Nullable ks: K*) {
        removeAll$(k1, k2, ks: _*)
    }

    /**
     * Creates and executes ad-hoc `SCAN` query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], kvp: KvPred): Iterable[(K, V)] = {
        assert(cls != null)
        assert(kvp != null)

        val q = value.cache[K, V]().queries().createScanQuery(kvp)

        (if (grid != null) q.projection(grid) else q).execute().get.map(e => (e.getKey, e.getValue))
    }

    /**
     * Creates and executes ad-hoc `SCAN` query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(@Nullable grid: ClusterGroup, kvp: KvPred)
        (implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(kvp != null)

        scan(grid, m.erasure.asInstanceOf[Class[V]], kvp)
    }

    /**
     * Creates and executes ad-hoc `SCAN` query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(cls: Class[_ <: V], kvp: KvPred): Iterable[(K, V)] = {
        assert(cls != null)
        assert(kvp != null)

        scan(null, cls, kvp)
    }

    /**
     * Creates and executes ad-hoc `SCAN` query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def scan(kvp: KvPred)(implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(kvp != null)

        scan(m.erasure.asInstanceOf[Class[V]], kvp)
    }

    /**
     * Creates and executes ad-hoc `SQL` query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(@Nullable grid: ClusterGroup, cls: Class[_ <: V], clause: String, args: Any*): Iterable[(K, V)] = {
        assert(cls != null)
        assert(clause != null)
        assert(args != null)

        val q = value.cache().queries().createSqlQuery(cls, clause)

        (if (grid != null) q.projection(grid) else q)
            .execute(args.asInstanceOf[Seq[Object]]: _*)
            .get.map(e => (e.getKey, e.getValue))
    }

    /**
     * Creates and executes ad-hoc `SQL` query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def sql(@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String): Iterable[(K, V)] = {
        assert(cls != null)
        assert(clause != null)

        sql(grid, cls, clause, Nil: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(@Nullable grid: ClusterGroup, clause: String, args: Any*)
        (implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(clause != null)
        assert(args != null)

        sql(grid, m.erasure.asInstanceOf[Class[V]], clause, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(cls: Class[_ <: V], clause: String, args: Any*): Iterable[(K, V)] = {
        assert(cls != null)
        assert(clause != null)

        sql(null.asInstanceOf[ClusterGroup], cls, clause, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sql(clause: String, args: Any*)(implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(clause != null)

        sql(m.erasure.asInstanceOf[Class[V]], clause, args: _*)
    }

    /**
     * Creates and executes ad-hoc `TEXT` query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String): Iterable[(K, V)] = {
        assert(cls != null)
        assert(clause != null)

        val q = value.cache().queries().createFullTextQuery(cls, clause)

        (if (grid != null) q.projection(grid) else q).execute().get.map(e => (e.getKey, e.getValue))
    }

    /**
     * Creates and executes ad-hoc `TEXT` query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(@Nullable grid: ClusterGroup, clause: String)(implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(clause != null)

        text(grid, m.erasure.asInstanceOf[Class[V]], clause)
    }

    /**
     * Creates and executes ad-hoc `TEXT` query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(cls: Class[_ <: V], clause: String): Iterable[(K, V)] = {
        assert(cls != null)
        assert(clause != null)

        text(null, cls, clause)
    }

    /**
     * Creates and executes ad-hoc `TEXT` query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @return Collection of cache key-value pairs.
     */
    def text(clause: String)(implicit m: Manifest[V]): Iterable[(K, V)] = {
        assert(clause != null)

        text(m.erasure.asInstanceOf[Class[V]], clause)
    }

    /**
     * Creates and executes ad-hoc `SCAN` transform query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def scanTransform[T](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], kvp: KvPred, trans: V => T):
        Iterable[(K, T)] = {
        assert(cls != null)
        assert(kvp != null)
        assert(trans != null)

        val q = value.cache[K, V]().queries().createScanQuery(kvp)

        toScalaItr[K, T]((if (grid != null) q.projection(grid) else q).execute(toRemoteTransformer[K, V, T](trans)).get)
    }

    /**
     * Creates and executes ad-hoc `SCAN` transform query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the global projection will be used.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def scanTransform[T](@Nullable grid: ClusterGroup, kvp: KvPred, trans: V => T)(implicit m: Manifest[V]):
        Iterable[(K, T)] = {
        assert(kvp != null)
        assert(trans != null)

        scanTransform(grid, m.erasure.asInstanceOf[Class[V]], kvp, trans)
    }

    /**
     * Creates and executes ad-hoc `SCAN` transform query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def scanTransform[T](cls: Class[_ <: V], kvp: KvPred, trans: V => T): Iterable[(K, T)] = {
        assert(cls != null)
        assert(kvp != null)
        assert(trans != null)

        scanTransform(null, cls, kvp, trans)
    }

    /**
     * Creates and executes ad-hoc `SCAN` transform query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def scanTransform[T](kvp: KvPred, trans: V => T)
        (implicit m: Manifest[V]): Iterable[(K, T)] = {
        assert(kvp != null)
        assert(trans != null)

        scanTransform(m.erasure.asInstanceOf[Class[V]], kvp, trans)
    }

    /**
     * Creates and executes ad-hoc `SQL` transform query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sqlTransform[T](@Nullable grid: ClusterGroup, cls: Class[_ <: V], clause: String,
        trans: V => T, args: Any*): Iterable[(K, T)] = {
        assert(cls != null)
        assert(clause != null)
        assert(trans != null)
        assert(args != null)

        val q = value.cache[K, V]().queries().createSqlQuery(cls, clause)

        toScalaItr((if (grid != null) q.projection(grid) else q)
            .execute(toRemoteTransformer[K, V, T](trans), args.asInstanceOf[Seq[Object]]: _*).get)
    }

    /**
     * Creates and executes ad-hoc `SQL` transform query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def sqlTransform[T](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        trans: V => T): Iterable[(K, T)] = {
        assert(cls != null)
        assert(clause != null)
        assert(trans != null)

        sqlTransform(grid, cls, clause, trans, Nil: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` transform query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sqlTransform[T](@Nullable grid: ClusterGroup, clause: String, trans: V => T, args: Any*)
        (implicit m: Manifest[V]): Iterable[(K, T)] = {
        assert(clause != null)
        assert(trans != null)
        assert(args != null)

        sqlTransform(grid, m.erasure.asInstanceOf[Class[V]], clause, trans, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` transform query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sqlTransform[T](cls: Class[_ <: V], clause: String, trans: V => T, args: Any*): Iterable[(K, T)] = {
        assert(cls != null)
        assert(clause != null)
        assert(trans != null)
        assert(args != null)

        sqlTransform(null, cls, clause, trans, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` transform query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @param args Optional list of query arguments.
     * @return Collection of cache key-value pairs.
     */
    def sqlTransform[T](clause: String, trans: V => T, args: Any*)
        (implicit m: Manifest[V]): Iterable[(K, T)] = {
        assert(clause != null)
        assert(trans != null)
        assert(args != null)

        sqlTransform(m.erasure.asInstanceOf[Class[V]], clause, trans, args: _*)
    }

    /**
     * Creates and executes ad-hoc `TEXT` transform query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def textTransform[T](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        trans: V => T): Iterable[(K, T)] = {
        assert(cls != null)
        assert(clause != null)
        assert(trans != null)

        val q = value.cache[K, V]().queries().createFullTextQuery(cls, clause)

        toScalaItr((if (grid != null) q.projection(grid) else q).execute(toRemoteTransformer[K, V, T](trans)).get)
    }

    /**
     * Creates and executes ad-hoc `TEXT` transform query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def textTransform[T](@Nullable grid: ClusterGroup, clause: String, trans: V => T)
        (implicit m: Manifest[V]): Iterable[(K, T)] = {
        assert(clause != null)
        assert(trans != null)

        textTransform(grid, m.erasure.asInstanceOf[Class[V]], clause, trans)
    }

    /**
     * Creates and executes ad-hoc `TEXT` transform query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def textTransform[T](cls: Class[_ <: V], clause: String, trans: V => T): Iterable[(K, T)] = {
        assert(cls != null)
        assert(clause != null)
        assert(trans != null)

        textTransform(null, cls, clause, trans)
    }

    /**
     * Creates and executes ad-hoc `TEXT` transform query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param trans Transform function that will be applied to each returned value.
     * @return Collection of cache key-value pairs.
     */
    def textTransform[T](clause: String, trans: V => T)
        (implicit m: Manifest[V]): Iterable[(K, T)] = {
        assert(clause != null)
        assert(trans != null)

        textTransform(m.erasure.asInstanceOf[Class[V]], clause, trans)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def scanReduce[R1, R2](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], kvp: KvPred,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2): R2 = {
        assert(cls != null)
        assert(kvp != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        val q = value.cache[K, V]().queries().createScanQuery(kvp)

        locRdc((if (grid != null) q.projection(grid) else q).execute(toEntryReducer(rmtRdc)).get)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def scanReduce[R1, R2](@Nullable grid: ClusterGroup, kvp: KvPred,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2)(implicit m: Manifest[V]): R2 = {
        assert(kvp != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        scanReduce(grid, m.erasure.asInstanceOf[Class[V]], kvp, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def scanReduce[R1, R2](cls: Class[_ <: V], kvp: KvPred,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2): R2 = {
        assert(cls != null)
        assert(kvp != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        scanReduce(null, cls, kvp, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def scanReduce[R1, R2](kvp: KvPred, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2)(implicit m: Manifest[V]): R2 = {
        assert(kvp != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        scanReduce(m.erasure.asInstanceOf[Class[V]], kvp, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @param args Optional list of query arguments.
     * @return Reduced value.
     */
    def sqlReduce[R1, R2](@Nullable grid: ClusterGroup, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2, args: Any*): R2 = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)
        assert(args != null)

        val q = value.cache[K, V]().queries().createSqlQuery(cls, clause)

        locRdc((if (grid != null) q.projection(grid) else q)
            .execute(toEntryReducer(rmtRdc), args.asInstanceOf[Seq[Object]]: _*).get)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def sqlReduce[R1, R2](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2): R2 = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        sqlReduce(grid, cls, clause, rmtRdc, locRdc, Nil: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @param args Optional list of query arguments.
     * @return Reduced value.
     */
    def sqlReduce[R1, R2](@Nullable grid: ClusterGroup, clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2, args: Any*)(implicit m: Manifest[V]): R2 = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)
        assert(args != null)

        sqlReduce(grid, m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, locRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @param args Optional list of query arguments.
     * @return Reduced value.
     */
    def sqlReduce[R1, R2](cls: Class[_ <: V], clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2, args: Any*): R2 = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)
        assert(args != null)

        sqlReduce(null, cls, clause, rmtRdc, locRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @param args Optional list of query arguments.
     * @return Reduced value.
     */
    def sqlReduce[R1, R2](clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2, args: Any*)(implicit m: Manifest[V]): R2 = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)
        assert(args != null)

        sqlReduce(m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, locRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def textReduce[R1, R2](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R1, locRdc: Iterable[R1] => R2): R2 = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        val q = value.cache[K, V]().queries().createFullTextQuery(cls, clause)

        locRdc((if (grid != null) q.projection(grid) else q).execute(toEntryReducer(rmtRdc)).get)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def textReduce[R1, R2](@Nullable grid: ClusterGroup, clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2)(implicit m: Manifest[V]): R2 = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        textReduce(grid, m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def textReduce[R1, R2](cls: Class[_ <: V], clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2): R2 = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        textReduce(null, cls, clause, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param locRdc Reduce function that will be called on local node.
     * @return Reduced value.
     */
    def textReduce[R1, R2](clause: String, rmtRdc: Iterable[(K, V)] => R1,
        locRdc: Iterable[R1] => R2)(implicit m: Manifest[V]): R2 = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(locRdc != null)

        textReduce(m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, locRdc)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def scanReduceRemote[R](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], kvp: KvPred,
        rmtRdc: Iterable[(K, V)] => R): Iterable[R] = {
        assert(cls != null)
        assert(kvp != null)
        assert(rmtRdc != null)

        val q = value.cache[K, V]().queries().createScanQuery(kvp)

        (if (grid != null) q.projection(grid) else q).execute(toEntryReducer(rmtRdc)).get
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the global projection will be used.
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def scanReduceRemote[R](@Nullable grid: ClusterGroup, kvp: KvPred,
        rmtRdc: Iterable[(K, V)] => R)(implicit m: Manifest[V]): Iterable[R] = {
        assert(kvp != null)
        assert(rmtRdc != null)

        scanReduceRemote(grid, m.erasure.asInstanceOf[Class[V]], kvp, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def scanReduceRemote[R](cls: Class[_ <: V], kvp: KvPred, rmtRdc: Iterable[(K, V)] => R): Iterable[R] = {
        assert(cls != null)
        assert(kvp != null)
        assert(rmtRdc != null)

        scanReduceRemote(null, cls, kvp, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `SCAN` reduce query on global projection returning its result.
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
     * @param kvp Filter to be used prior to returning key-value pairs to user. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def scanReduceRemote[R](kvp: KvPred, rmtRdc: Iterable[(K, V)] => R)(implicit m: Manifest[V]): Iterable[R] = {
        assert(kvp != null)
        assert(rmtRdc != null)

        scanReduceRemote(m.erasure.asInstanceOf[Class[V]], kvp, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param args Optional list of query arguments.
     * @return Collection of reduced values.
     */
    def sqlReduceRemote[R](@Nullable grid: ClusterGroup, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R, args: Any*): Iterable[R] = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(args != null)

        val q = value.cache[K, V]().queries().createSqlQuery(cls, clause)

        (if (grid != null) q.projection(grid) else q)
            .execute(toEntryReducer(rmtRdc), args.asInstanceOf[Seq[Object]]: _*).get
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def sqlReduceRemote[R](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R): Iterable[R] = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)

        sqlReduceRemote(grid, cls, clause, rmtRdc, Nil: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param args Optional list of query arguments.
     * @return Collection of reduced values.
     */
    def sqlReduceRemote[R](@Nullable grid: ClusterGroup, clause: String, rmtRdc: Iterable[(K, V)] => R,
        args: Any*)(implicit m: Manifest[V]): Iterable[R] = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(args != null)

        sqlReduceRemote(grid, m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param args Optional list of query arguments.
     * @return Collection of reduced values.
     */
    def sqlReduceRemote[R](cls: Class[_ <: V], clause: String, rmtRdc: Iterable[(K, V)] => R,
        args: Any*): Iterable[R] = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)
        assert(args != null)

        sqlReduceRemote(null, cls, clause, rmtRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `SQL` reduce query on global projection returning its result.
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
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @param args Optional list of query arguments.
     * @return Collection of reduced values.
     */
    def sqlReduceRemote[R](clause: String, rmtRdc: Iterable[(K, V)] => R, args: Any*)
        (implicit m: Manifest[V]): Iterable[R] = {
        assert(clause != null)
        assert(rmtRdc != null)
        assert(args != null)

        sqlReduceRemote(m.erasure.asInstanceOf[Class[V]], clause, rmtRdc, args: _*)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param cls Query values class. Since cache can, in general, contain values of any subtype of `V`
     *     query needs to know the exact type it should operate on.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def textReduceRemote[R](@Nullable grid: ClusterGroup = null, cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R): Iterable[R] = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)

        val q = value.cache[K, V]().queries().createFullTextQuery(cls, clause)

        (if (grid != null) q.projection(grid) else q).execute(toEntryReducer(rmtRdc)).get
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on given projection returning its result.
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
     * @param grid Grid projection on which this query will be executed. If `null` the
     *     global projection will be used.
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def textReduceRemote[R](@Nullable grid: ClusterGroup, clause: String, rmtRdc: Iterable[(K, V)] => R)
        (implicit m: Manifest[V]): Iterable[R] = {
        assert(clause != null)
        assert(rmtRdc != null)

        textReduceRemote(grid, m.erasure.asInstanceOf[Class[V]], clause, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def textReduceRemote[R](cls: Class[_ <: V], clause: String,
        rmtRdc: Iterable[(K, V)] => R): Iterable[R] = {
        assert(cls != null)
        assert(clause != null)
        assert(rmtRdc != null)

        textReduceRemote(null, cls, clause, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `TEXT` reduce query on global projection returning its result.
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
     * @param clause Query text clause. See `GridCacheQuery` for more details.
     * @param rmtRdc Reduce function that will be called on each remote node.
     * @return Collection of reduced values.
     */
    def textReduceRemote[R](clause: String, rmtRdc: Iterable[(K, V)] => R)
        (implicit m: Manifest[V]): Iterable[R] = {
        assert(clause != null)
        assert(rmtRdc != null)

        textReduceRemote(m.erasure.asInstanceOf[Class[V]], clause, rmtRdc)
    }

    /**
     * Creates and executes ad-hoc `SQL` fields query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Optional grid projection on which this query will be executed. If `null` the
     *      global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @param args Optional list of query arguments.
     * @return Sequence of sequences of field values.
     */
    def sqlFields(@Nullable grid: ClusterGroup, clause: String, args: Any*): IndexedSeq[IndexedSeq[Any]] = {
        assert(clause != null)
        assert(args != null)

        val q = value.cache[K, V]().queries().createSqlFieldsQuery(clause)

        (if (grid != null) q.projection(grid) else q).execute(args.asInstanceOf[Seq[Object]]: _*)
            .get.toIndexedSeq.map((s: java.util.List[_]) => s.toIndexedSeq)
    }

    /**
     * Creates and executes ad-hoc `SQL` no-arg fields query on given projection returning its result.
     *
     * Note that if query is executed more than once (potentially with different
     * arguments) it is more performant to create query via standard mechanism
     * and execute it multiple times with different arguments. The analogy is
     * similar to JDBC `PreparedStatement`. Note also that this function will return
     * all results at once without pagination and therefore memory limits should be
     * taken into account.
     *
     * @param grid Optional grid projection on which this query will be executed. If `null` the
     *      global projection will be used.
     * @param clause Query SQL clause. See `GridCacheQuery` for more details.
     * @return Sequence of sequences of field values.
     */
    def sqlFields(@Nullable grid: ClusterGroup = null, clause: String): IndexedSeq[IndexedSeq[Any]] = {
        assert(clause != null)

        sqlFields(grid, clause, Nil: _*)
    }
}

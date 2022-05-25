package org.apache.ignite.gatling.builder.cache

import io.gatling.core.session.Expression

final class Cache(requestName: Expression[String], cacheName: Expression[String]) {
    def put[K, V](key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
        CachePutActionBuilder(requestName, cacheName, key, value)
    def putAll[K, V](map: Expression[Map[K, V]]): CachePutAllActionBuilder[K, V] =
        CachePutAllActionBuilder(requestName, cacheName, map)
    def get[K, V](key: Expression[K]): CacheGetActionBuilder[K, V] =
        CacheGetActionBuilder(requestName, cacheName, key)
    def getAll[K, V](keys: Expression[Set[K]]): CacheGetAllActionBuilder[K, V] =
        CacheGetAllActionBuilder(requestName, cacheName, keys)
    def remove[K](key: Expression[K]): CacheRemoveActionBuilder[K] =
        CacheRemoveActionBuilder(requestName, cacheName, key)
    def removeAll[K](keys: Expression[Set[K]]): CacheRemoveAllActionBuilder[K] =
        CacheRemoveAllActionBuilder(requestName, cacheName, keys)
    def invoke[K, V, T](key: Expression[K]): CacheInvokeActionBuilderBase[K, V, T] =
        CacheInvokeActionBuilderBase[K, V, T](requestName, cacheName, key)
}

package org.apache.ignite.gatling.builder.cache

import io.gatling.core.session.Expression
import org.apache.ignite.gatling.builder.cache

final class Cache(requestName: Expression[String], cacheName: Expression[String]) {
    def put[K, V](key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
        cache.CachePutActionBuilder(requestName, cacheName, key, value)
}

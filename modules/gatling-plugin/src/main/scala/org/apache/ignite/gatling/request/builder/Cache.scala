package org.apache.ignite.gatling.request.builder

import io.gatling.core.session.Expression

final class Cache(requestName: Expression[String], cacheName: Expression[String]) {
    def put[K, V](key: Expression[K], value: Expression[V]): CachePutActionBuilder[K, V] =
        CachePutActionBuilder(requestName, cacheName, key, value)
}

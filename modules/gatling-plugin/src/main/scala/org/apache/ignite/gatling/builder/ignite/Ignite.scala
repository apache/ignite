package org.apache.ignite.gatling.builder.ignite

import io.gatling.core.session.Expression
import org.apache.ignite.gatling.builder.cache.Cache
import org.apache.ignite.gatling.builder.ignite

final class Ignite(requestName: Expression[String]) {
    def cache(cacheName: Expression[String]): Cache = new Cache(requestName, cacheName)
    def create[K, V](cacheName: Expression[String]): CreateCacheActionBuilderBase[K, V] = CreateCacheActionBuilderBase(requestName, cacheName)
    def start: StartClientActionBuilder = ignite.StartClientActionBuilder(requestName)
    def close: CloseClientActionBuilder = ignite.CloseClientActionBuilder(requestName)

    implicit def createCacheActionBuilderSimpleConfigStep2CreateCacheActionBuilder[K, V](step: CreateCacheActionBuilderSimpleConfigStep): CreateCacheActionBuilder[K, V] =
        step.createCacheActionBuilder
}

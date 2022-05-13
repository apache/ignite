package org.apache.ignite.gatling.request.builder

import io.gatling.core.session.Expression

final class Ignite(requestName: Expression[String]) {
    def cache(cacheName: Expression[String]): Cache = new Cache(requestName, cacheName)
    def create(cacheName: Expression[String]): CreateCacheActionBuilder = CreateCacheActionBuilder(requestName, cacheName)
    def startClient: StartClientActionBuilder = StartClientActionBuilder(requestName)
    def closeClient: CloseClientActionBuilder = CloseClientActionBuilder(requestName)
}

/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.{IgniteFuture, IgniteInClosure}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheAsyncApiExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
    
    /** Cache name. */
    private val CACHE_NAME = ScalarCacheAsyncApiExample.getClass.getSimpleName
    
    scalar(CONFIG) {
        println()
        println(">>> Cache asynchronous API example started.")

        val cache = createCache$[Integer, String](CACHE_NAME)

        try {
            // Enable asynchronous mode.
            val asyncCache = cache.withAsync()

            // Execute several puts asynchronously.
            val t = (0 until 10).map(i => {
                asyncCache.put(i, String.valueOf(i))

                asyncCache.future().asInstanceOf[IgniteFuture[_]]
            }).foreach(_.get())

            // Execute get operation asynchronously.
            asyncCache.get(1)

            // Asynchronously wait for result.
            asyncCache.future[String].listen(new IgniteInClosure[IgniteFuture[String]] {
                @impl def apply(fut: IgniteFuture[String]) {
                    println("Get operation completed [value=" + fut.get + ']')
                }
            })
        }
        finally {
            cache.close()
        }
    }
}

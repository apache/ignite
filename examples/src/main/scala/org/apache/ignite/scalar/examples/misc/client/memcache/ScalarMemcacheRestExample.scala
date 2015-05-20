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

package org.apache.ignite.scalar.examples.misc.client.memcache

import org.apache.ignite.examples.misc.client.memcache.MemcacheRestExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import net.spy.memcached.{BinaryConnectionFactory, MemcachedClient}

import java.io.IOException
import java.net.InetSocketAddress
import java.util.Arrays

/**
 * This example shows how to use Memcache client for manipulating Ignite cache.
 * <p>
 * Ignite implements Memcache binary protocol and it is available if
 * REST is enabled on the node.
 * Remote nodes should always be started using [[MemcacheRestExampleNodeStartup]].
 */
object ScalarMemcacheRestExample extends App {
    /** Cache name. */
    private val CACHE_NAME = ScalarMemcacheRestExample.getClass.getSimpleName

    /** Hostname for client connection. */
    private val host = "localhost"

    /** Port number for client connection. */
    private val port = 11211

    scalar(MemcacheRestExampleNodeStartup.configuration()) {
        var client: MemcachedClient = null

        try {
            println()
            println(">>> Memcache REST example started.")

            val cache = createCache$[String, AnyRef](CACHE_NAME)

            try {
                client = startMemcachedClient(host, port)

                if (client.add("strKey", 0, "strVal").get)
                    println(">>> Successfully put string value using Memcache client.")

                println(">>> Getting value for 'strKey' using Ignite cache API: " + cache.get("strKey"))
                println(">>> Getting value for 'strKey' using Memcache client: " + client.get("strKey"))

                if (client.delete("strKey").get)
                    println(">>> Successfully removed string value using Memcache client.")

                println(">>> Current cache size: " + cache.size() + " (expected: 0).")

                if (client.add("intKey", 0, 100).get)
                    println(">>> Successfully put integer value using Memcache client.")

                println(">>> Getting value for 'intKey' using Ignite cache API: " + cache.get("intKey"))
                println(">>> Getting value for 'intKey' using Memcache client: " + client.get("intKey"))

                if (client.delete("intKey").get)
                    println(">>> Successfully removed integer value using Memcache client.")

                println(">>> Current cache size: " + cache.size() + " (expected: 0).")

                val l = long$("atomicLong", true, 10)

                if (client.incr("atomicLong", 5, 0) == 15)
                    println(">>> Successfully incremented atomic long by 5.")

                println(">>> New atomic long value: " + l.incrementAndGet + " (expected: 16).")

                if (client.decr("atomicLong", 3, 0) == 13)
                    println(">>> Successfully decremented atomic long by 3.")

                println(">>> New atomic long value: " + l.decrementAndGet + " (expected: 12).")
            }
            finally {
                cache.close()
            }
        }
        finally {
            if (client != null)
                client.shutdown()
        }
    }

    /**
     * Creates Memcache client that uses binary protocol and connects to Ignite.
     *
     * @param host Hostname.
     * @param port Port number.
     * @return Client.
     * @throws IOException If connection failed.
     */
    @throws(classOf[IOException])
    private def startMemcachedClient(host: String, port: Int): MemcachedClient = {
        assert(host != null)
        assert(port > 0)

        new MemcachedClient(new BinaryConnectionFactory, Arrays.asList(new InetSocketAddress(host, port)))
    }
}

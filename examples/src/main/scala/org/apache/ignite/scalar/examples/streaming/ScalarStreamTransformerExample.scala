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

package org.apache.ignite.scalar.examples.streaming

import org.apache.ignite.Ignition
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.stream.StreamTransformer

import javax.cache.processor.MutableEntry
import java.lang.{Long => JavaLong}
import java.util.Random

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *  <li>Start a few nodes using [[ExampleNodeStartup]] or by starting remote nodes as specified below.</li>
 *  <li>Start streaming using [[ScalarStreamTransformerExample]].</li>
 * </ul>
 * <p>
 * You should start remote nodes by running [[ExampleNodeStartup]] in another JVM.
 */
object ScalarStreamTransformerExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Random number generator. */
    private val RAND = new Random

    /** Range within which to generate numbers. */
    private val RANGE = 1000

    scalar(CONFIG) {
        Ignition.setClientMode(true)

        if (ExamplesUtils.hasServerNodes(ignite$)) {
            val stmCache = createCache$[Integer, JavaLong]("randomNumbers",
                indexedTypes = Seq(classOf[Integer], classOf[JavaLong]))

            try {
                val stmr = dataStreamer$[Integer, JavaLong](stmCache.getName)

                try {
                    stmr.allowOverwrite(true)

                    stmr.receiver(new StreamTransformer[Integer, JavaLong] {
                        @impl def process(e: MutableEntry[Integer, JavaLong], args: AnyRef*): AnyRef = {
                            val v = e.getValue

                            e.setValue(if (v == null)
                                    1L
                                else
                                    v + 1)

                            null
                        }
                    })

                    for (i <- 0 until 1000000) {
                        stmr.addData(RAND.nextInt(RANGE), 1L)

                        if (i % 500000 == 0)
                            println("Number of tuples streamed into Ignite: " + i)
                    }
                }
                finally {
                    stmr.close()
                }

                val top10Qry = new SqlFieldsQuery("select _key, _val from Long order by _val desc limit 10")
                
                val top10 = stmCache.query(top10Qry).getAll
                
                println("Top 10 most popular numbers:")

                ExamplesUtils.printQueryResults(top10)
            }
            finally {
                stmCache.close()
            }
        }
    }
}

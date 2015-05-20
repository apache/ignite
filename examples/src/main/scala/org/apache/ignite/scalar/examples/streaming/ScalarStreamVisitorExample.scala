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

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.stream.StreamVisitor
import org.apache.ignite.{IgniteCache, Ignition}

import java.io.Serializable
import java.util.{Map => JavaMap, Random}


/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 * <li>Start a few nodes using [[ExampleNodeStartup]] or by starting remote nodes as specified below.</li>
 * <li>Start streaming using [[ScalarStreamVisitorExample]].</li>
 * </ul>
 * <p>
 * You should start remote nodes by running [[ExampleNodeStartup]] in another JVM.
 */
object ScalarStreamVisitorExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Random number generator. */
    private val RAND = new Random

    /** The list of instruments. */
    private val INSTRUMENTS = Array("IBM", "GOOG", "MSFT", "GE", "EBAY", "YHOO", "ORCL", "CSCO", "AMZN", "RHT")

    /** The list of initial instrument prices. */
    private val INITIAL_PRICES = Array(194.9, 893.49, 34.21, 23.24, 57.93, 45.03, 44.41, 28.44, 378.49, 69.50)

    scalar(CONFIG) {
        Ignition.setClientMode(true)

        if (ExamplesUtils.hasServerNodes(ignite$)) {
            val mktCache = createCache$[String, Double]("marketTicks")

            try {
                val instCache = createCache$[String, Instrument]("instCache",
                    indexedTypes = Seq(classOf[String], classOf[Instrument]))

                try {
                    val mktStmr = ignite$.dataStreamer[String, Double](mktCache.getName)

                    try {
                        mktStmr.receiver(new StreamVisitor[String, Double] {
                            @impl def apply(cache: IgniteCache[String, Double], e: JavaMap.Entry[String, Double]) {
                                val symbol = e.getKey
                                val tick = e.getValue
                                var inst = instCache.get(symbol)

                                if (inst == null)
                                    inst = new Instrument(symbol)

                                inst.update(tick)

                                instCache.put(symbol, inst)
                            }
                        })

                        for (i <- 0 until 10000000) {
                            val idx = RAND.nextInt(INSTRUMENTS.length)

                            val price = round2(INITIAL_PRICES(idx) + RAND.nextGaussian)

                            mktStmr.addData(INSTRUMENTS(idx), price)

                            if (i % 500000 == 0)
                                println("Number of tuples streamed into Ignite: " + i)
                        }
                    }
                    finally {
                        if (mktStmr != null) mktStmr.close()
                    }

                    val top3qry = new SqlFieldsQuery("select symbol, " +
                        "(latest - open) from Instrument order by (latest - open) desc limit 3")

                    val top3 = instCache.query(top3qry).getAll

                    println("Top performing financial instruments: ")

                    ExamplesUtils.printQueryResults(top3)
                }
                finally {
                    if (instCache != null) instCache.close()
                }
            }
            finally {
                mktCache.close()
            }
        }
    }

    /**
     * Rounds double value to two significant signs.
     *
     * @param value value to be rounded.
     * @return rounded double value.
     */
    private def round2(value: Double): Double = {
        Math.floor(100 * value + 0.5) / 100
    }

}


/**
 * Financial instrument.
 *
 * @param symb Instrument symbol.
 */
class Instrument(symb: String) extends Serializable {
    /** Instrument symbol. */
    @QuerySqlField(index = true) private final val symbol: String = symb

    /** Open price. */
    @QuerySqlField(index = true) private var open: Double = .0

    /** Close price. */
    @QuerySqlField(index = true) private var latest: Double = .0

    /**
     * Updates this instrument based on the latest market tick price.
     *
     * @param price Latest price.
     */
    def update(price: Double) {
        if (open == 0)
            open = price

        this.latest = price
    }
}

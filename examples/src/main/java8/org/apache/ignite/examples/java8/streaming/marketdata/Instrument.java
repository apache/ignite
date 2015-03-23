/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.java8.streaming.marketdata;

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;

/**
 * Financial instrument.
 */
public class Instrument implements Serializable {
    /** Instrument symbol. */
    @QuerySqlField(index = true)
    private final String symbol;

    /** Open price. */
    @QuerySqlField(index = true)
    private double open;

    /** High price. */
    private double high;

    /** Low price. */
    private double low = Long.MAX_VALUE;

    /** Close price. */
    @QuerySqlField(index = true)
    private double latest;

    /**
     * @param symbol Symbol.
     */
    Instrument(String symbol) {
        this.symbol = symbol;
    }

    /**
     * Updates this instrument based on the latest price.
     *
     * @param price Latest price.
     */
    public void update(double price) {
        if (open == 0)
            open = price;

        high = Math.max(high, price);
        low = Math.min(low, price);
        this.latest = price;
    }

    /**
     * @return Symbol.
     */
    public String symbol() {
        return symbol;
    }

    /**
     * @return Open price.
     */
    public double open() {
        return open;
    }

    /**
     * @return High price.
     */
    public double high() {
        return high;
    }

    /**
     * @return Low price.
     */
    public double low() {
        return low;
    }

    /**
     * @return Close price.
     */
    public double latest() {
        return latest;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return "Instrument [symbol=" + symbol + ", latest=" + latest + ", change=" + (latest - open) + ']';
    }
}

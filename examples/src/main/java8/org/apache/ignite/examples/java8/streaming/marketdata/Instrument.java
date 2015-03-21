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

/**
 *
 */
public class Instrument {
    /** Instrument symbol. */
    private final String symbol;

    /** Open price. */
    private volatile double open;

    /** High price. */
    private volatile double high;

    /** Low price. */
    private volatile double low = Long.MAX_VALUE;

    /** Close price. */
    private volatile double close;

    /**
     * @param symbol Symbol.
     */
    Instrument(String symbol) {
        this.symbol = symbol;
    }

    /**
     * @return Copy of this instance.
     */
    public synchronized Instrument copy() {
        Instrument res = new Instrument(symbol);

        res.open = open;
        res.high = high;
        res.low = low;
        res.close = close;

        return res;
    }

    /**
     * Updates this bar with last price.
     *
     * @param price Price.
     */
    public synchronized void update(double price) {
        if (open == 0)
            open = price;

        high = Math.max(high, price);
        low = Math.min(low, price);
        close = price;
    }

    /**
     * Updates this bar with next bar.
     *
     * @param instrument Next bar.
     */
    public synchronized void update(Instrument instrument) {
        if (open == 0)
            open = instrument.open;

        high = Math.max(high, instrument.high);
        low = Math.min(low, instrument.low);
        close = instrument.close;
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
    public double close() {
        return close;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return "Bar [symbol=" + symbol + ", open=" + open + ", high=" + high + ", low=" + low +
            ", close=" + close + ']';
    }
}

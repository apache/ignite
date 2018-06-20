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

package org.apache.ignite.examples.stockengine.domain;


public class Quote {
    private final Instrument instrument;
    private final double bid;
    private final double ask;
    private final long quoteTime;

    public Quote(Instrument instrument, double bid, double ask, long quoteTime) {
        this.instrument = instrument;
        this.bid = bid;
        this.ask = ask;
        this.quoteTime = quoteTime;
    }

    public Instrument getInstrument() {
        return instrument;
    }

    public double getBid() {
        return bid;
    }

    public double getAsk() {
        return ask;
    }

    public long getQuoteTime() {
        return quoteTime;
    }
}

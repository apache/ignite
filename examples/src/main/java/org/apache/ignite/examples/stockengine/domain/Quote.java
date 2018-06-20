package org.apache.ignite.examples.stockengine.domain;

import org.apache.ignite.examples.stockengine.domain.Instrument;

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

package org.apache.ignite.examples.stockengine.domain;

import org.apache.ignite.examples.stockengine.domain.Instrument;

public class Position {
    private final Instrument instrument;
    private final double quantity;
    private final int user;

    public Position(Instrument instrument, double quantity, int user) {
        this.instrument = instrument;
        this.quantity = quantity;
        this.user = user;
    }
}

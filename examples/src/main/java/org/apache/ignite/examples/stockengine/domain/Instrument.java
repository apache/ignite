package org.apache.ignite.examples.stockengine.domain;

public class Instrument {
    public static Instrument EUR_USD = new Instrument("EUR/USD");

    private final String name;

    public Instrument(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

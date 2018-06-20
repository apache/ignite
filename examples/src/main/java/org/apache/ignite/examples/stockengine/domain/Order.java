package org.apache.ignite.examples.stockengine.domain;

import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.OptionType;
import org.apache.ignite.examples.stockengine.domain.Side;
import org.apache.ignite.examples.stockengine.domain.State;

public class Order {
    private final long id;
    private final int userId;
    private final OptionType type;
    private final Side side;
    private final Instrument instrument;
    private final long expirationDate;
    private final double price;
    private final State state;

    public Order(
            long id,
            int userId,
            OptionType type,
            Side side,
            Instrument instrument,
            long expirationDate,
            double price,
            State state
    ) {
        this.id = id;
        this.userId = userId;
        this.type = type;
        this.side = side;
        this.instrument = instrument;
        this.expirationDate = expirationDate;
        this.price = price;
        this.state = state;
    }

    public int getUserId() {
        return userId;
    }

    public OptionType getType() {
        return type;
    }

    public Side getSide() {
        return side;
    }

    public long getExpirationDate() {
        return expirationDate;
    }

    public double getPrice() {
        return price;
    }

    public long getId() {
        return id;
    }

    public Instrument getInstrument() {
        return instrument;
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", userId=" + userId +
                ", type=" + type +
                ", side=" + side +
                ", instrument=" + instrument +
                ", expirationDate=" + expirationDate +
                ", price=" + price +
                ", state=" + state +
                '}';
    }
}

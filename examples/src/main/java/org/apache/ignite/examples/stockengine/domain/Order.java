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

import java.util.Objects;

public class Order {
    private final long id;
    private final int userId;
    private final Side side;
    private final Instrument instrument;
    private final long expirationDate;
    private final double price;
    private final double premium;
    private State state;

    public Order(
            long id,
            int userId,
            Side side,
            Instrument instrument,
            long expirationDate,
            double price,
            double premium, State state
    ) {
        this.id = id;
        this.userId = userId;
        this.side = side;
        this.instrument = instrument;
        this.expirationDate = expirationDate;
        this.price = price;
        this.premium = premium;
        this.state = state;
    }

    public int getUserId() {
        return userId;
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

    public double getPremium() {
        return premium;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return id == order.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", userId=" + userId +
                ", side=" + side +
                ", instrument=" + instrument +
                ", expirationDate=" + expirationDate +
                ", price=" + price +
                ", state=" + state +
                '}';
    }
}

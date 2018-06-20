package org.apache.ignite.examples.stockengine.approach3;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

import java.util.Objects;

public class OrderKey {
    private final long id;

    @AffinityKeyMapped
    private final int userId;


    public OrderKey(long id, int userId) {
        this.id = id;
        this.userId = userId;
    }

    public long getId() {
        return id;
    }

    public int getUserId() {
        return userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderKey orderKey = (OrderKey) o;
        return id == orderKey.id &&
                userId == orderKey.userId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userId);
    }
}

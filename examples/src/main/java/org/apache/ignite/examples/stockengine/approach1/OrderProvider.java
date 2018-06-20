package org.apache.ignite.examples.stockengine.approach1;

import org.apache.ignite.examples.stockengine.domain.Order;

import java.util.Collection;

public interface OrderProvider {
    public Collection<Order> getActiveOrders();

    public void listenNewOrders(Listener listener);

    interface Listener {
        void notify(Order order);
    }
}

package org.apache.ignite.examples.stockengine.approach1;

import org.apache.ignite.examples.stockengine.domain.OptionType;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Quote;

import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NaiveEngine {
    protected ConcurrentSkipListSet<Order> timeBasedExpiration = new ConcurrentSkipListSet<Order>(new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            long l = o1.getExpirationDate() - o2.getExpirationDate();
            return l > 0 ? 1 : l == 0 ? 0 : -1;
        }
    });

    protected ConcurrentSkipListSet<Order> buyOneTouchExpiration = new ConcurrentSkipListSet<Order>(new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            double d = o1.getPrice() - o2.getPrice();

            return d > 0 ? 1 : d == 0 ? 0 : -1;
        }
    });

    protected ConcurrentSkipListSet<Order> sellOneTouchExpiration = new ConcurrentSkipListSet<Order>(new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            double d = o2.getPrice() - o1.getPrice();

            return d > 0 ? 1 : d == 0 ? 0 : -1;
        }
    });

    protected final BlockingQueue<Quote> quotes = new ArrayBlockingQueue<>(10000);

    protected final AtomicBoolean stopped = new AtomicBoolean();

    public void addOrder(Order order) {
        assert order != null && order.getType() != null : order;

        if (order.getType() == OptionType.ONE_TOUCH || order.getType() == OptionType.NO_TOUCH) {
            switch (order.getSide()) {
                case SELL:
                    sellOneTouchExpiration.add(order);

                    break;

                case BUY:
                    buyOneTouchExpiration.add(order);

                    break;
            }
        }

        timeBasedExpiration.add(order);
    }

    public void start() {
        new Thread(new Runnable() {
            private Quote lastQuote = null;

            @Override
            public void run() {
                try {
                    while (!stopped.get()) {
                        Quote poll = quotes.poll(200, TimeUnit.MILLISECONDS);

                        if (poll != null)
                            while (handleSellOrders(poll) | handleBuyOrders(poll));

                        if (poll == null)
                            poll = lastQuote; //get historical quote

                        if (poll == null)
                            continue;

                        lastQuote = poll;

                        while (handleAnyOrder(poll));
                    }
                } catch (InterruptedException e) {
                    // Some exception hangling.
                }
            }
        }).start();
    }

    private boolean handleAnyOrder(Quote poll) {
        Order first = timeBasedExpiration.first();

        if (poll.getQuoteTime() >= first.getExpirationDate()) {
            switch (first.getType()) {
                case NO_TOUCH:
                    executeOrder(first, true);

                    break;

                case ONE_TOUCH:
                    executeOrder(first, false);

                    break;

                case SIMPLE:
                    double orderPrice = first.getPrice();

                    switch (first.getSide()) {
                        case BUY:
                            executeOrder(first, orderPrice > poll.getAsk());

                            break;

                        case SELL:
                            executeOrder(first, orderPrice < poll.getBid());

                            break;
                    }

                    break;
            }

            return true;
        }

        return false;
    }

    private boolean handleBuyOrders(Quote poll) {
        Order first = buyOneTouchExpiration.first();

        if (first == null)
            return false;

        double orderPrice = first.getPrice();

        double offerPrice = poll.getAsk();

        if (first.getType() == OptionType.ONE_TOUCH) {
            if (orderPrice >= offerPrice) {
                executeOrder(first, true);

                return true;
            }

        } //NO_TOUCH
        else if (orderPrice <= offerPrice) {
            executeOrder(first, false);

            return true;
        }

        return false;
    }

    private boolean handleSellOrders(Quote poll) {
        Order first = sellOneTouchExpiration.first();

        if (first == null)
            return false;

        double orderPrice = first.getPrice();

        double offerPrice = poll.getBid();

        if (first.getType() == OptionType.ONE_TOUCH) {
            if (orderPrice <= offerPrice) { //execute order
                executeOrder(first, true);

                return true;
            }
        } //NO_TOUCH
        else if (orderPrice >= offerPrice) {
            executeOrder(first, false);

            return true;
        }

        return false;
    }

    protected abstract void executeOrder(Order order, boolean success);
}

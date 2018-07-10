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

package org.apache.ignite.examples.stockengine.approach1;

import org.apache.ignite.examples.stockengine.QuoteProvider;
import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Quote;

import java.util.Comparator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NaiveEngine {
    protected ConcurrentSkipListSet<Order> timeBasedExpirationOptions = new ConcurrentSkipListSet<Order>(new Comparator<Order>() {
        @Override
        public int compare(Order o1, Order o2) {
            long l = o1.getExpirationDate() - o2.getExpirationDate();
            return l > 0 ? 1 : l == 0 ? 0 : -1;
        }
    });

    /** Quotes queue. */
    protected final BlockingQueue<Quote> quotes = new ArrayBlockingQueue<>(10000);

    /** Stopped flag. */
    protected final AtomicBoolean stopped = new AtomicBoolean();

    /** Quote provider. */
    private QuoteProvider quoteProvider;

    /**
     * @param order Order.
     */
    public void addOrder(Order order) {
        assert order != null;

        timeBasedExpirationOptions.add(order);
    }

    /**
     *
     */
    public void start() {
        quoteProvider = new QuoteProvider();

        quoteProvider.registerListener(Instrument.EUR_USD, new QuoteProvider.Listener() {
            @Override public void listen(Quote quote) {
                try {
                    quotes.offer(quote, 1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        quoteProvider.start();

        new Thread(new Runnable() {
            private Quote lastQuote = null;

            @Override public void run() {
                try {
                    while (!stopped.get()) {
                        Quote poll = quotes.poll(200, TimeUnit.MILLISECONDS);

                        if (poll == null)
                            poll = lastQuote; //get historical quote

                        if (poll == null)
                            continue;

                        lastQuote = poll;

                        while (handleTimeBasedOrder(poll));
                    }
                } catch (InterruptedException e) {
                    // Some exception hangling.
                }
            }
        }).start();
    }

    /**
     *
     */
    public void stop() {
        stopped.set(true);

        quoteProvider.stop();
    }

    /**
     * @param quote New quote to process.
     */
    private boolean handleTimeBasedOrder(Quote quote) {
        if (timeBasedExpirationOptions.isEmpty())
            return false;

        Order first = timeBasedExpirationOptions.first();

        if (quote.getQuoteTime() >= first.getExpirationDate()) {
            double orderPrice = first.getPrice();

            switch (first.getSide()) {
                case BUY:
                    executeOrder(first, orderPrice > quote.getAsk());

                    break;

                case SELL:
                    executeOrder(first, orderPrice < quote.getBid());

                    break;
            }

            return true;
        }

        return false;
    }

    protected abstract void executeOrder(Order order, boolean success);
}

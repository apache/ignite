package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.stockengine.QuoteProvider;
import org.apache.ignite.examples.stockengine.approach1.NaiveEngine;
import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.Quote;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;
import java.util.concurrent.TimeUnit;

public class DurableEngine extends NaiveEngine implements Service, Printer {
    @IgniteInstanceResource
    private Ignite ignite;

    protected void executeOrder(Order order, boolean success) {
        System.err.println("EXECUTE " + order);
    }

    @Override public void printState() {
        System.err.println("timeBasedExpiration=" + timeBasedExpiration.toString());
        System.err.println("timeBasedExpiration.size()=" + timeBasedExpiration.size());
        System.err.println("buyOneTouchExpiration=" + buyOneTouchExpiration.toString());
        System.err.println("sellOneTouchExpiration=" + sellOneTouchExpiration.toString());
    }

    @Override public void cancel(ServiceContext ctx) {
        stopped.set(true);
    }

    @Override public void init(ServiceContext ctx) throws Exception {
        QuoteProvider quoteProvider = new QuoteProvider();

        quoteProvider.registerListener(Instrument.EUR_USD, new QuoteProvider.Listener() {
            @Override
            public void listen(Quote quote) {
                try {
                    quotes.offer(quote, 1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override public void execute(ServiceContext ctx) throws Exception {
        ContinuousQuery<Long, Order> qry = createContinousQuery();

        QueryCursor<Cache.Entry<Long, Order>> order1 = ignite.<Long, Order>cache("orders").query(qry);

        for (Cache.Entry<Long, Order> e : order1)
            addOrder(e.getValue());

        super.start();
    }

    @NotNull
    private ContinuousQuery<Long, Order> createContinousQuery() {
        ContinuousQuery<Long, Order> qry = new ContinuousQuery<>();

        qry.setLocalListener(iterable -> {
            for (CacheEntryEvent<? extends Long, ? extends Order> e : iterable)
                addOrder(e.getValue());
        });

        qry.setRemoteFilterFactory(
                (Factory<CacheEntryEventFilter<Long, Order>>) () ->
                        (CacheEntryEventSerializableFilter<Long, Order>) event ->
                                event.getEventType() != EventType.REMOVED &&
                                        event.getEventType() != EventType.EXPIRED &&
                                        event.getValue().getState() != State.CLOSED
        );

        qry.setInitialQuery(new ScanQuery<>((IgniteBiPredicate<Long, Order>)
                (aLong, order) -> order.getState() != State.CLOSED));

        return qry;
    }
}

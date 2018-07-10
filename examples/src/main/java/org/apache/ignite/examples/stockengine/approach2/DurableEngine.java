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

package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.stockengine.approach1.NaiveEngine;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.examples.stockengine.domain.User;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;

/**
 *
 */
public class DurableEngine extends NaiveEngine implements Service, Engine {
    /** Ignite instance. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** Continuous query. */
    protected volatile QueryCursor<Cache.Entry<OrderKey, Order>> continuousQuery;

    /** {@inheritDoc} */
    protected void executeOrder(Order order, boolean success) {
        //Remove from all sets.
        timeBasedExpirationOptions.remove(order);

        try (Transaction t = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            IgniteCache<OrderKey, Order> orders = ignite.cache("orders");

            OrderKey key = new OrderKey(order.getId(), order.getUserId());

            Order curOrder = orders.get(key);

            if (curOrder.getState() == State.ACTIVE) {
                System.err.println("EXECUTE order=" + order + ", success? " + success);

                curOrder.setState(State.CLOSED);

                orders.put(key, curOrder);

                if (success) {
                    IgniteCache<Integer, User> users = ignite.cache("users");

                    User user = users.get(order.getUserId());

                    user.updateBalance(order.getPremium());

                    users.put(user.getId(), user);
                }
            }
            else
                System.err.println("SKIPPED result=" + success + ", order=" + order);

            t.commit();
        }
    }

    /** {@inheritDoc} */
    @Override public int orderInWork() {
        return timeBasedExpirationOptions.size();
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        super.stop();

        if (continuousQuery != null)
            continuousQuery.close();
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        System.out.println("Service initialized.");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Service started.");

        ContinuousQuery<OrderKey, Order> qry = createContinousQuery();

        continuousQuery = ignite.<Long, Order>cache("orders").query(qry);

        for (Cache.Entry<OrderKey, Order> e : continuousQuery)
            addOrder(e.getValue());

        super.start();
    }

    /**
     *
     */
    @NotNull
    protected ContinuousQuery<OrderKey, Order> createContinousQuery() {
        ContinuousQuery<OrderKey, Order> qry = new ContinuousQuery<>();

        qry.setLocalListener(iterable -> {
            for (CacheEntryEvent<? extends OrderKey, ? extends Order> e : iterable)
                addOrder(e.getValue());
        });

        qry.setRemoteFilterFactory(
                (Factory<CacheEntryEventFilter<OrderKey, Order>>) () ->
                        (CacheEntryEventSerializableFilter<OrderKey, Order>) event ->
                                event.getEventType() != EventType.REMOVED &&
                                        event.getEventType() != EventType.EXPIRED &&
                                        event.getValue().getState() == State.ACTIVE
        );

        qry.setInitialQuery(new ScanQuery<>((IgniteBiPredicate<OrderKey, Order>)
                (aLong, order) -> order.getState() == State.ACTIVE));

        return qry;
    }
}

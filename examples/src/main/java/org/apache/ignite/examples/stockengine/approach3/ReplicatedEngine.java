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

package org.apache.ignite.examples.stockengine.approach3;

import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.events.EventType;
import org.apache.ignite.examples.stockengine.approach2.DurableEngine;
import org.apache.ignite.examples.stockengine.approach2.OrderKey;
import org.apache.ignite.examples.stockengine.domain.Order;
import org.apache.ignite.examples.stockengine.domain.State;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.services.ServiceContext;
import org.jetbrains.annotations.NotNull;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;

/**
 *
 */
public class ReplicatedEngine extends DurableEngine {
    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        System.out.println("Service started.");

        ContinuousQuery<OrderKey, Order> qry = createContinousQuery();

        continuousQuery = ignite.<Long, Order>cache("orders").query(qry);

        initialFill();

        ignite.events().localListen(event -> {
                    System.err.println("New event: " + event);

                    initialFill();

                    return true;
                } ,
                EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED);

        super.start();
    }

    /** {@inheritDoc} */
    @NotNull protected ContinuousQuery<OrderKey, Order> createContinousQuery() {
        ContinuousQuery<OrderKey, Order> qry = new ContinuousQuery<>();

        qry.setRemoteFilterFactory(
                (Factory<CacheEntryEventFilter<OrderKey, Order>>) () ->
                        (CacheEntryEventSerializableFilter<OrderKey, Order>) event -> {
                            if (event.getEventType() != javax.cache.event.EventType.REMOVED &&
                                    event.getEventType() != javax.cache.event.EventType.EXPIRED &&
                                    event.getValue().getState() == State.ACTIVE)
                                addOrder(event.getValue());

                            return false;
                        }
        );

        qry.setLocalListener(ignored -> { });

        return qry;
    }


    /**
     * Initial structure fill.
     */
    private void initialFill() {
        Affinity<Object> affinity = ignite.affinity("orders");

        int[] ints = affinity.allPartitions(ignite.cluster().localNode());

        for (int partId : ints) {
            for (Cache.Entry<OrderKey, Order> e :
                    ignite.cache("orders").query(new ScanQuery<OrderKey, Order>(partId)).getAll()) {
                Order order = e.getValue();

                if (order.getState() == State.ACTIVE)
                    addOrder(order);
            }
        }
    }
}

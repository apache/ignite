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

package org.apache.ignite.internal.test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Optional;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mockito;

/**
 * Listener which wraps another one to inhibit events.
 */
public class WatchListenerInhibitor implements WatchListener {
    /** Inhibited events. */
    private final ArrayList<WatchEvent> inhibitEvents = new ArrayList<>();

    /** Inhibit flag. */
    private boolean inhibit = false;

    /** Wrapped listener. */
    private WatchListener realListener;

    /**
     * Creates the specific listener which can inhibit events for real metastorage listener.
     *
     * @param ignite Ignite.
     * @return Listener inhibitor.
     * @throws Exception If something wrong when creating the listener inhibitor.
     */
    public static WatchListenerInhibitor metastorageEventsInhibitor(Ignite ignite)
            throws Exception {
        //TODO: IGNITE-15723 After a component factory will be implemented, need to got rid of reflection here.
        MetaStorageManager metaMngr = (MetaStorageManager) ReflectionUtils.tryToReadFieldValue(
                IgniteImpl.class,
                "metaStorageMgr",
                (IgniteImpl) ignite
        ).get();
    
        assertNotNull(metaMngr);
    
        WatchAggregator aggregator = (WatchAggregator) ReflectionUtils.tryToReadFieldValue(
                MetaStorageManager.class,
                "watchAggregator",
                metaMngr
        ).get();

        assertNotNull(aggregator);

        WatchAggregator aggregatorSpy = Mockito.spy(aggregator);

        WatchListenerInhibitor inhibitor = new WatchListenerInhibitor();
    
        doAnswer(mock -> {
            Optional<AggregatedWatch> op = (Optional<AggregatedWatch>) mock.callRealMethod();
        
            assertTrue(op.isPresent());
        
            inhibitor.setRealListener(op.get().listener());
        
            return Optional.of(new AggregatedWatch(op.get().keyCriterion(), op.get().revision(),
                    inhibitor));
        }).when(aggregatorSpy).watch(anyLong(), any());

        IgniteTestUtils.setFieldValue(metaMngr, "watchAggregator", aggregatorSpy);

        // Redeploy metastorage watch. The Watch inhibitor will be used after.
        metaMngr.unregisterWatch(-1);

        return inhibitor;
    }

    /**
     * Default constructor.
     */
    private WatchListenerInhibitor() {
    }

    /**
     * Sets a wrapped listener.
     *
     * @param realListener Listener to wrap.
     */
    private void setRealListener(WatchListener realListener) {
        this.realListener = realListener;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean onUpdate(WatchEvent evt) {
        if (!inhibit) {
            return realListener.onUpdate(evt);
        }

        return inhibitEvents.add(evt);
    }

    /** {@inheritDoc} */
    @Override public synchronized void onError(Throwable e) {
        realListener.onError(e);
    }

    /**
     * Starts inhibit events.
     */
    public synchronized void startInhibit() {
        inhibit = true;
    }

    /**
     * Stops inhibit events.
     */
    public synchronized void stopInhibit() {
        inhibit = false;
    
        for (WatchEvent evt : inhibitEvents) {
            realListener.onUpdate(evt);
        }

        inhibitEvents.clear();
    }
}

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

package org.apache.ignite.examples.stockengine;

import org.apache.ignite.examples.stockengine.domain.Instrument;
import org.apache.ignite.examples.stockengine.domain.Quote;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuoteProvider {
    public interface Listener {
        void listen(Quote quote);
    }

    /** Listeners. */
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    /** Stopped flag. */
    private final AtomicBoolean stopped = new AtomicBoolean();

    public void registerListener(Instrument instrument, Listener listener) {
        listeners.add(listener);
    }

    public void start() {
        new Thread(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom current = ThreadLocalRandom.current();

                while (!stopped.get()) {
                    try {
                        for (Listener listener : listeners) {
                            double noise = current.nextDouble(0.05);

                            listener.listen(new Quote(new Instrument("EU"),
                                    1.049 - noise, 1.050 + noise , System.currentTimeMillis()));
                        }

                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public void stop() {
        stopped.set(true);
    }
}
